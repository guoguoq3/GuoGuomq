package org.guoguo.broker.util;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
消费者组持久化时将 在持久化文件中的消费者会有唯一性
 */
@Slf4j
@Component
public class OffsetPersistUtil {
    private final MqConfigProperties mqConfigProperties;

    //当前正在写入的持久化文件
    private File currentPersistFile;
    //文件写入流
    private BufferedWriter writer;

    // 内存缓存：记录已持久化的唯一键（groupId:topic），避免重复写入（可选优化，减少文件IO）
    private final Map<String, String> persistedUniqueKeyCache = new ConcurrentHashMap<>();

    @Autowired
    public OffsetPersistUtil(MqConfigProperties mqConfigProperties) {
        this.mqConfigProperties = mqConfigProperties;

    }


    /**
     * 不是启动时执行 而是一个消费者组订阅时调用这个方法 获取是否存在位点 如果有就直接恢复到其消费者组文件中
     */

    public void init(ConsumerGroup consumerGroup,String topic) {
        try {
            //若是持久化目录不存在就创建一个
            File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
            if (!persistDir.exists()) {
                boolean mkdirSuccess = persistDir.mkdirs();
                if (mkdirSuccess) {
                    log.info("WhisperMQ==============> 创建消费者位点持久化目录成功 ：{}", persistDir.getAbsolutePath());
                } else {
                    log.info("WhisperMQ==============> 创建费者位点持久化持久化目录失败{}", persistDir.getAbsolutePath());
                    throw new RuntimeException("持久化目录创建失败，Broker 启动异常");
                }
            }
            //查找最新的持久化文件 如是有历史文件 优先用最新的 没有就创建文件
            currentPersistFile = findLatestPersistFile(persistDir);
            if (currentPersistFile == null) {
                currentPersistFile = createNewPersistFile(persistDir);
            }

            writer = Files.newBufferedWriter(currentPersistFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

            // 从历史文件中恢复消息到内存

            recoverOffsetByGroupAndTopic(consumerGroup, topic);

        } catch (Exception e) {
            log.error("WhisperMQ异常，初始化消费者位点持久化目录失败======================>", e);
            throw new RuntimeException("初始化消费者位点持久化目录失败，Broker 启动异常");

        }
    }

    /**
     * 写入位点：按“groupId:topic”唯一键存储，避免重复
     * @param groupId 消费者组ID
     * @param topic 主题
     * @param messageId 最新消费的消息ID（位点）
     */
    public synchronized void writeMessage(String groupId, String topic, String messageId) {
        try {
            // 唯一键：groupId:topic（同一组同一主题只存最新位点）
            String uniqueKey = groupId + ":" + topic;

            //  检查文件大小：超过最大限制则滚动文件
            if (currentPersistFile.length() >= mqConfigProperties.getMaxFileSize()) {
                log.info("WhisperMQ==============> 当前位点文件超过最大大小（{}），创建新文件",
                        mqConfigProperties.getMaxFileSize());
                // 关闭旧流
                writer.close();
                // 新建文件
                currentPersistFile = createNewPersistFile(new File(mqConfigProperties.getOffsetPersistPath()));
                // 初始化新流
                writer = Files.newBufferedWriter(
                        currentPersistFile.toPath(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE
                );
                // 清空内存缓存（新文件重新记录）todo：消息位点久远是否还回溯
                persistedUniqueKeyCache.clear();
            }

            //避免重复写入 保证唯一性
            if(persistedUniqueKeyCache.containsKey(uniqueKey)&&persistedUniqueKeyCache.get(uniqueKey).equals(messageId)){
                log.debug("WhisperMQ==============> 位点无更新，跳过写入（唯一键：{}，位点：{}）",
                        uniqueKey, messageId);
                return;
            }


            // ORDER_GROUP:ORDER_TOPIC|1699999999999
            String messageStr= uniqueKey + "|" + messageId;
            //写入文件
            writer.write(messageStr);
            //换行 方便后续按行读取
            writer.newLine();
            //强制刷新 立即写入
            writer.flush();
            // 更新内存缓存（记录最新唯一键和位点）
            persistedUniqueKeyCache.put(uniqueKey, messageId);

            log.info("WhisperMQ 消息持久化成功：文件={}，消息ID={}", currentPersistFile.getName(), groupId);

        }catch (Exception e){
            log.error("WhisperMQ 消息持久化失败，消息ID={}", groupId, e);
            //todo：失败重试的机制

        }
    }

    /**
     * 查找最新的位点文件（按修改时间降序）
     */
    private File findLatestPersistFile(File persistDir) {
        File[] files = persistDir.listFiles((dir, name) ->
                name.startsWith("WhisperMQ-offset-persist-") && name.endsWith(".log")
        );
        if (files == null || files.length == 0) {
            return null;
        }

        // 按修改时间降序，取第一个（最新）
        return Arrays.stream(files)
                .max(Comparator.comparingLong(File::lastModified))
                .orElse(null);
    }

    /**
     * 创建新的持久化文件
     */
    private File createNewPersistFile(File persistDir) {
        String fileName = "WhisperMQ-offset-persist-" + System.currentTimeMillis() + ".log";
        File newFile = new File(persistDir, fileName);
        try {
            boolean createSuccess = newFile.createNewFile();
            if (createSuccess) {
                log.info("WhisperMQ==============> 新消费者位点持久化文件创建成功：{}", newFile.getAbsolutePath());
                return newFile;
            } else {
                log.info("WhisperMQ==============> 新消费者位点持久化文件创建失败：{}", newFile.getAbsolutePath());
                throw new IOException("新消费者位点持久化文件创建失败");
            }
        } catch (Exception e) {
            log.error("WhisperMQ 新消费者位点持久化文件创建失败", e);
            throw new RuntimeException("新消费者位点持久化文件创建失败", e);
        }

    }
    /**
     * 从所有历史文件中恢复位点到内存
     * 注意在这些消息的恢复过程中都是单线程的 不会涉及到多并发这时系统还未开始正常处理外部请求，不需要并发处理
     * 但是可以 todo：多线程来进行消息恢复
     */
    public void recoverOffsetByGroupAndTopic(ConsumerGroup consumerGroup, String topic) {

        String groupId = consumerGroup.getGroupId();
        String targetUniqueKey = groupId + ":" + topic;


        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
        if (!persistDir.exists()) {
            log.info("WhisperMQ 消费位点目录不存在，无需恢复");
            return;
        }

        // 1. 获取所有位点文件（按修改时间升序，确保先恢复旧文件）
        File[] files = persistDir.listFiles((dir, name) ->
                name.startsWith("WhisperMQ-offset-persist-") && name.endsWith(".log")
        );
        if (files == null || files.length == 0) {
            log.info("WhisperMQ 无消费位点文件，无需恢复");
            return ;
        }

        // 按修改时间升序排列（先处理旧文件，后处理新文件，确保位点覆盖正确）
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));


        //最新位点
        String latestOffset = null;

        // 遍历所有文件，查找目标唯一键的最新位点
        for (File file : files) {
            try (BufferedReader reader = Files.newBufferedReader(
                    file.toPath(),
                    StandardCharsets.UTF_8
            )) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // 4. 按“|”分割唯一键和位点（与写入格式一致）
                    String[] parts = line.split("\\|", 2);
                    if (parts.length != 2) {
                        log.warn("WhisperMQ==============> 位点文件解析异常，无效行（文件：{}，行：{}）",
                                file.getName(), line);
                        continue;
                    }

                    String fileUniqueKey = parts[0];
                    String fileOffset = parts[1];

                    // 5. 匹配目标唯一键：更新最新位点（后读的文件位点覆盖前读的）
                    if (targetUniqueKey.equals(fileUniqueKey)) {
                        latestOffset = fileOffset;
                        log.debug("WhisperMQ==============> 从文件{}中读取到位点（唯一键：{}，位点：{}）",
                                file.getName(), fileUniqueKey, fileOffset);
                    }
                }
            } catch (Exception e) {
                log.error("WhisperMQ==============> 读取位点文件{}失败", file.getName(), e);
                // 单个文件失败不影响整体，继续处理下一个文件
            }
        }

        // 6. 若找到最新位点，更新到消费者组内存
        if (latestOffset != null) {
            consumerGroup.getTopicOffsetMap().put(topic, latestOffset);
            // 更新内存缓存（避免后续重复写入）
            persistedUniqueKeyCache.put(targetUniqueKey, latestOffset);
            log.info("WhisperMQ==============> 成功恢复位点（组：{}，主题：{}，位点：{}）",
                    groupId, topic, latestOffset);
        } else {
            log.info("WhisperMQ==============> 未找到目标位点（组：{}，主题：{}）",
                    groupId, topic);
        }
    }



    /**
     * 回溯所有组的所有位点 存在意义在于集群宕机我直接不用传入直接恢复 集群恢复
     */
    public Map<String, Map<String, String>> recoverAllOffset() {
        Map<String, Map<String, String>> result = new HashMap<>();

        File persistDir = new File(mqConfigProperties.getOffsetPersistPath());
        if (!persistDir.exists()) {
            log.info("WhisperMQ==============> 位点目录不存在，无需恢复全量位点");
            return result;
        }

        // 1. 获取所有位点文件（按修改时间升序）
        File[] files = persistDir.listFiles((dir, name) ->
                name.startsWith("WhisperMQ-offset-persist-") && name.endsWith(".log")
        );
        if (files == null || files.length == 0) {
            log.info("WhisperMQ==============> 无位点文件，无需恢复全量位点");
            return result;
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));

        // 临时存储：key=唯一键（groupId|topic），value=最新位点（确保后读的覆盖前读的）
        Map<String, String> allLatestOffsetMap = new HashMap<>();

        // 2. 遍历所有文件，收集所有唯一键的最新位点
        for (File file : files) {
            try (BufferedReader reader = Files.newBufferedReader(
                    file.toPath(),
                    StandardCharsets.UTF_8
            )) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // 按"|"分割groupId、topic和offset
                    String[] parts = line.split("\\|", 3);
                    if (parts.length != 3) {
                        log.warn("WhisperMQ==============> 全量恢复：无效行（文件：{}，行：{}）",
                                file.getName(), line);
                        continue;
                    }

                    String groupId = parts[0];
                    String topic = parts[1];
                    String offset = parts[2];

                    // 构造唯一键
                    String uniqueKey = groupId + "|" + topic;
                    // 覆盖更新：后读的文件位点优先级更高
                    allLatestOffsetMap.put(uniqueKey, offset);
                }
            } catch (Exception e) {
                log.error("WhisperMQ==============> 全量恢复：读取文件{}失败", file.getName(), e);
            }
        }

        // 3. 将收集的最新位点整理成结果格式
        for (Map.Entry<String, String> entry : allLatestOffsetMap.entrySet()) {
            String uniqueKey = entry.getKey();
            String offset = entry.getValue();

            // 解析唯一键：groupId|topic
            String[] keyParts = uniqueKey.split("\\|", 2);
            if (keyParts.length != 2) {
                log.warn("WhisperMQ==============> 全量恢复：无效唯一键（{}）", uniqueKey);
                continue;
            }
            String groupId = keyParts[0];
            String topic = keyParts[1];

            // 整理成嵌套Map结构
            result.computeIfAbsent(groupId, k -> new HashMap<>()).put(topic, offset);

            // 更新内存缓存
            persistedUniqueKeyCache.put(uniqueKey, offset);

            log.debug("WhisperMQ==============> 全量恢复位点（组：{}，主题：{}，位点：{}）",
                    groupId, topic, offset);
        }

        log.info("WhisperMQ==============> 全量位点恢复完成，共恢复{}个唯一键的位点", result.size());
        return result;
    }

    /**
     * 关闭资源（Broker关闭时调用）
     */
    public synchronized void close() {
        if (writer != null) {
            try {
                writer.close();
                log.info("WhisperMQ==============> 位点写入流关闭成功");
            } catch (IOException e) {
                log.error("WhisperMQ==============> 位点写入流关闭失败", e);
            }
        }
    }
}