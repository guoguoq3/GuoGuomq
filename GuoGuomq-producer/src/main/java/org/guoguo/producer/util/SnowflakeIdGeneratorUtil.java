package org.guoguo.producer.util;

import org.springframework.stereotype.Component;

@Component
public class  SnowflakeIdGeneratorUtil {
    // 起始时间戳，2020-01-01 00:00:00
    private final long startTimeStamp = 1577836800000L;

    // 机器ID所占位数
    private final long workerIdBits = 5L;
    // 数据中心ID所占位数
    private final long dataCenterIdBits = 5L;
    // 序列号所占位数
    private final long sequenceBits = 12L;

    // 机器ID最大值 31
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 数据中心ID最大值 31
    private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

    // 机器ID向左移位数
    private final long workerIdShift = sequenceBits;
    // 数据中心ID向左移位数
    private final long dataCenterIdShift = sequenceBits + workerIdBits;
    // 时间戳向左移位数
    private final long timestampShift = sequenceBits + workerIdBits + dataCenterIdBits;
    // 序列号掩码 4095
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private final long workerId;
    private final long dataCenterId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public SnowflakeIdGeneratorUtil() {
        // 若使用默认值，可在这里初始化（例如workerId=1，dataCenterId=1）
        this(1L, 1L);
    }
    // 构造函数
    public SnowflakeIdGeneratorUtil(long workerId, long dataCenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("Worker ID 不能大于 " + maxWorkerId + " 或小于 0");
        }
        if (dataCenterId > maxDataCenterId || dataCenterId < 0) {
            throw new IllegalArgumentException("数据中心 ID 不能大于 " + maxDataCenterId + " 或小于 0");
        }
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
    }

    // 生成下一个ID
    public synchronized  long nextId() {
        long currentTimestamp = System.currentTimeMillis();

        // 处理时钟回拨问题
        if (currentTimestamp < lastTimestamp) {
            throw new RuntimeException("时钟回拨，拒绝生成ID " + (lastTimestamp - currentTimestamp) + " 毫秒");
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 序列号用尽，等待下一毫秒
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // 时间戳改变，重置序列号
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;

        return ((currentTimestamp - startTimeStamp) << timestampShift) |
                (dataCenterId << dataCenterIdShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    // 等待下一毫秒
    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }


}    