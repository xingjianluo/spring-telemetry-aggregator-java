package com.github.xingjianluo.springtelemetryaggregatorjava.window;

import com.github.xingjianluo.springtelemetryaggregatorjava.config.AggregatorProperties;
import com.github.xingjianluo.springtelemetryaggregatorjava.dto.GroupStats;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectedEvent;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.springtelemetryaggregatorjava.model.TelemetryRecord;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 窗口生命周期、水位线、迟到兜底、维度/容量限制与并发一致性的核心。
 *
 * <p><b>时间窗口</b>：按 {@code telemetry.window-millis} 固定对齐，事件 e 属于
 * {@code [floor(e.ts/W)*W, +W)}；<b>水位线</b> {@code WM = max(已见ts) - allowedLateness}；
 * 当窗口结束时刻 {@code ws+W <= WM} 时该窗口视为关闭，落到关闭窗口的数据判定为过晚，
 * 进入迟到兜底而非并入当前窗口。</p>
 *
 * <p><b>并发</b>：所有窗口与身份表操作都在 {@code windows} 同一把锁内完成（检查+提交原子），
 * 保证并发提交结果与逐个串行一致；迟到兜底存储使用独立锁，加锁顺序固定为 windows → late。</p>
 */
@Component
public class WindowManager {

    private final AggregatorProperties props;

    /** 窗口起点 -> 窗口状态，TreeMap 便于按时间范围查询与淘汰最旧窗口。 */
    private final TreeMap<Long, WindowState> windows = new TreeMap<>();

    /** id -> 身份信息：去重与语义冲突的唯一依据（含迟到兜底身份）。 */
    private final java.util.HashMap<String, IdentityInfo> identities = new java.util.HashMap<>();

    /** 过晚数据兜底存储，仅用于留痕排查，不参与窗口聚合。 */
    private final List<TelemetryRecord> lateSink = new ArrayList<>();

    private final Object lateLock = new Object();

    private volatile long watermark = 0L;

    /** 拒绝/兜底事件留痕（最新在前），有界。 */
    private final Deque<RejectedEvent> rejections = new ArrayDeque<>();
    private final Object rejectionLock = new Object();

    public WindowManager(AggregatorProperties props) {
        this.props = props;
    }

    /**
     * 提交一条已归一记录。
     *
     * <p>判定顺序：① 同 id 已存在 → 指纹相同=重复，指纹不同=语义冲突；
     * ② 更新水位线并判定是否过晚 → 过晚进入兜底；
     * ③ 维度上限校验；④ 窗口容量校验；⑤ 原子提交。</p>
     */
    public SubmitResult submit(TelemetryRecord r) {
        long now = System.currentTimeMillis();
        long ws = windowStart(r.timestamp());

        synchronized (windows) {
            // ① 身份判定（对正常窗口与迟到兜底全局生效）
            IdentityInfo existing = identities.get(r.id());
            if (existing != null) {
                if (existing.fingerprint().equals(r.fingerprint())) {
                    return new SubmitResult(IngestStatus.DUPLICATE, null,
                            existing.windowStart(),
                            "幂等重复：id=" + r.id() + " 与已接收记录完全一致，计数与耗时不变");
                }
                String detail = "语义冲突：id=" + r.id() + " 已存在但内容不同，拒绝本次提交";
                appendRejection(new RejectedEvent(now, r.timestamp(), existing.windowStart(),
                        r.id(), IngestStatus.REJECTED_CONFLICT, RejectReason.ID_CONFLICT, detail));
                return new SubmitResult(IngestStatus.REJECTED_CONFLICT, RejectReason.ID_CONFLICT,
                        existing.windowStart(), detail);
            }

            // ② 水位线推进（由已观察到的最大事件时间驱动，与到达顺序无关）
            long candidate = r.timestamp() - props.getAllowedLatenessMillis();
            if (candidate > watermark) {
                watermark = candidate;
            }
            boolean tooLate = ws + props.getWindowMillis() <= watermark;
            if (tooLate) {
                String detail = "过晚兜底：事件窗口[" + ws + "," + (ws + props.getWindowMillis())
                        + ") 已被水位线 " + watermark + " 关闭，不并入当前窗口";
                identities.put(r.id(), new IdentityInfo(ws, r.fingerprint(), true));
                synchronized (lateLock) {
                    lateSink.add(r);
                    if (lateSink.size() > props.getMaxRejectedEvents()) {
                        lateSink.remove(0);
                    }
                }
                appendRejection(new RejectedEvent(now, r.timestamp(), ws, r.id(),
                        IngestStatus.REROUTED_LATE, RejectReason.TOO_LATE, detail));
                return new SubmitResult(IngestStatus.REROUTED_LATE, RejectReason.TOO_LATE, ws, detail);
            }

            // 淘汰最旧窗口，保证窗口数量有界
            while (windows.size() >= props.getMaxWindowsRetained()) {
                evictOldestWindow();
            }
            WindowState state = windows.computeIfAbsent(ws,
                    k -> new WindowState(ws, ws + props.getWindowMillis()));

            // ③ 维度上限：计算本记录将新增多少不同的 key=value 维度
            int newDimensions = 0;
            for (Map.Entry<String, String> e : r.attributes().entrySet()) {
                if (!state.getDimensions().contains(e.getKey() + "=" + e.getValue())) {
                    newDimensions++;
                }
            }
            if (state.getDimensions().size() + newDimensions > props.getMaxDimensionsPerWindow()) {
                String detail = "维度膨胀：窗口[" + ws + "] 维度数将达到 "
                        + (state.getDimensions().size() + newDimensions)
                        + "，超过上限 " + props.getMaxDimensionsPerWindow();
                appendRejection(new RejectedEvent(now, r.timestamp(), ws, r.id(),
                        IngestStatus.REJECTED_LIMIT, RejectReason.TOO_MANY_DIMENSIONS, detail));
                return new SubmitResult(IngestStatus.REJECTED_LIMIT, RejectReason.TOO_MANY_DIMENSIONS,
                        ws, detail);
            }

            // ④ 单窗口容量上限
            if (state.getCount() >= props.getMaxRecordsPerWindow()) {
                String detail = "窗口已满：窗口[" + ws + "] 记录数达到上限 "
                        + props.getMaxRecordsPerWindow();
                appendRejection(new RejectedEvent(now, r.timestamp(), ws, r.id(),
                        IngestStatus.REJECTED_LIMIT, RejectReason.WINDOW_FULL, detail));
                return new SubmitResult(IngestStatus.REJECTED_LIMIT, RejectReason.WINDOW_FULL, ws, detail);
            }

            // ⑤ 原子提交：身份登记与窗口聚合在同一临界区内完成
            state.accept(r);
            identities.put(r.id(), new IdentityInfo(ws, r.fingerprint(), false));
            return new SubmitResult(IngestStatus.ACCEPTED, null, ws,
                    "已接收并并入窗口[" + ws + "," + (ws + props.getWindowMillis()) + ")");
        }
    }

    /** 查询 [from,to) 范围内的窗口快照，groupBy 指定按哪些属性维度分组（为空则只给总量）。 */
    public Map<Long, WindowSnapshot> query(long from, long to, List<String> groupBy) {
        synchronized (windows) {
            Map<Long, WindowSnapshot> out = new TreeMap<>();
            // subMap 视图本身按 key 升序
            Map<Long, WindowState> sub = windows.subMap(from, true, to, false);
            long wm = watermark;
            for (WindowState s : sub.values()) {
                out.put(s.getStart(), toSnapshot(s, wm, groupBy));
            }
            return out;
        }
    }

    /** 当前水位线（epoch 毫秒）。 */
    public long currentWatermark() {
        return watermark;
    }

    /** 最近被拒绝/兜底的事件留痕（最新在前）。 */
    public List<RejectedEvent> recentRejections(int limit) {
        synchronized (rejectionLock) {
            int n = Math.max(0, Math.min(limit, props.getMaxRejectedEvents()));
            List<RejectedEvent> out = new ArrayList<>(Math.min(n, rejections.size()));
            int taken = 0;
            for (RejectedEvent e : rejections) {
                if (taken++ >= n) {
                    break;
                }
                out.add(e);
            }
            return out;
        }
    }

    /** 迟到兜底存储中的记录数（便于本地验证）。 */
    public int lateSinkSize() {
        synchronized (lateLock) {
            return lateSink.size();
        }
    }

    // ---- 内部方法 -------------------------------------------------------

    private long windowStart(long timestamp) {
        long w = props.getWindowMillis();
        return Math.floorDiv(timestamp, w) * w;
    }

    private void evictOldestWindow() {
        // 调用方已持有 windows 锁
        Map.Entry<Long, WindowState> oldest = windows.pollFirstEntry();
        if (oldest == null) {
            return;
        }
        // 窗口淘汰后其身份记录一并移除：被淘汰窗口早已超出保留范围，
        // 其重复提交将依据水位线走迟到兜底，语义在文档中明确。
        for (TelemetryRecord r : oldest.getValue().records()) {
            IdentityInfo info = identities.get(r.id());
            if (info != null && !info.late() && info.windowStart() == oldest.getKey()) {
                identities.remove(r.id());
            }
        }
    }

    private WindowSnapshot toSnapshot(WindowState s, long wm, List<String> groupBy) {
        // 调用方已持有 windows 锁，records() 读取安全
        Map<String, long[]> raw = new TreeMap<>();
        // long[]{count, sum, min, max, durationCount}
        for (TelemetryRecord r : s.records()) {
            String key = groupKey(r, groupBy);
            long[] agg = raw.computeIfAbsent(key,
                    k -> new long[]{0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, 0L});
            agg[0]++;
            if (r.durationNanos() != null) {
                long d = r.durationNanos();
                agg[1] += d;
                agg[2] = Math.min(agg[2], d);
                agg[3] = Math.max(agg[3], d);
                agg[4]++;
            }
        }
        Map<String, GroupStats> groups = new TreeMap<>();
        raw.forEach((key, agg) -> groups.put(key, new GroupStats(
                agg[0], agg[1],
                agg[4] == 0 ? 0L : agg[2],
                agg[4] == 0 ? 0L : agg[3],
                agg[4] == 0 ? null : (double) agg[1] / agg[4])));
        boolean closed = s.getEnd() <= wm;
        return new WindowSnapshot(s.getStart(), s.getEnd(), wm, closed, s.getCount(), groups);
    }

    private static String groupKey(TelemetryRecord r, List<String> groupBy) {
        if (groupBy == null || groupBy.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < groupBy.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String v = r.attributes().get(groupBy.get(i));
            sb.append(groupBy.get(i)).append('=').append(v == null ? "-" : v);
        }
        return sb.toString();
    }

    private void appendRejection(RejectedEvent e) {
        synchronized (rejectionLock) {
            rejections.addFirst(e);
            while (rejections.size() > props.getMaxRejectedEvents()) {
                rejections.removeLast();
            }
        }
    }

    private record IdentityInfo(long windowStart, String fingerprint, boolean late) {
    }

    /** 提交结论。 */
    public record SubmitResult(
            IngestStatus status,
            RejectReason reason,
            Long windowStart,
            String detail
    ) {
    }

    /** 窗口快照（供服务层转视图）。 */
    public record WindowSnapshot(
            long start,
            long end,
            long watermark,
            boolean closed,
            long totalCount,
            Map<String, GroupStats> groups
    ) {
    }
}
