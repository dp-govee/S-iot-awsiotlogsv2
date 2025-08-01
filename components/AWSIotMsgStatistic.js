import { CloudWatchClient, GetMetricStatisticsCommand } from "@aws-sdk/client-cloudwatch";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, GetItemCommand, ScanCommand } from "@aws-sdk/client-dynamodb";
import { Readable } from 'stream';

// 初始化客户端
const cloudwatchClient = new CloudWatchClient({ region: "us-east-1" });
const s3Client = new S3Client({ region: "us-east-1" });
const dynamoClient = new DynamoDBClient({ region: "us-east-1" });

// 配置
const S3_BUCKET = "govee-logs";
const S3_KEY_PREFIX = "iotcore-logs/";
const MESSAGE_STATS_TABLE = "IoTMessageStats"; // 可选的消息统计表

/**
 * 从CloudWatch获取指定时间段的消息统计
 * @param {string} metricName - 指标名称
 * @param {Date} startTime - 开始时间
 * @param {Date} endTime - 结束时间
 * @param {number} period - 时间段（秒）
 * @returns {Promise<number>} - 消息数量
 */
async function getMessagesFromCloudWatch(metricName, startTime, endTime, period = 86400) {
    try {
        const params = {
            Namespace: 'AWS/IoT',
            MetricName: metricName,
            StartTime: startTime,
            EndTime: endTime,
            Period: period,
            Statistics: ['Sum'],
            Dimensions: [] // 不指定维度获取全局统计
        };

        const response = await cloudwatchClient.send(new GetMetricStatisticsCommand(params));
        const totalMessages = response.Datapoints.length > 0 ? response.Datapoints[0].Sum : 0;
        
        console.log(`CloudWatch ${metricName}: ${totalMessages.toLocaleString()}`);
        return totalMessages;
    } catch (error) {
        console.error(`获取CloudWatch指标 ${metricName} 失败:`, error);
        return 0;
    }
}

/**
 * 获取每日入站消息总数
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<number>} - 入站消息数量
 */
async function getDailyInboundMessages(date) {
    const startTime = new Date(date);
    startTime.setHours(0, 0, 0, 0);
    
    const endTime = new Date(date);
    endTime.setHours(23, 59, 59, 999);
    
    return await getMessagesFromCloudWatch('PublishIn.Success', startTime, endTime);
}

/**
 * 获取每日出站消息总数
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<number>} - 出站消息数量
 */
async function getDailyOutboundMessages(date) {
    const startTime = new Date(date);
    startTime.setHours(0, 0, 0, 0);
    
    const endTime = new Date(date);
    endTime.setHours(23, 59, 59, 999);
    
    return await getMessagesFromCloudWatch('PublishOut.Success', startTime, endTime);
}

/**
 * 获取每日连接统计
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Object>} - 连接统计数据
 */
async function getDailyConnectionStats(date) {
    const startTime = new Date(date);
    startTime.setHours(0, 0, 0, 0);
    
    const endTime = new Date(date);
    endTime.setHours(23, 59, 59, 999);
    
    const [successConnections, failedConnections, disconnections] = await Promise.all([
        getMessagesFromCloudWatch('Connect.Success', startTime, endTime),
        getMessagesFromCloudWatch('Connect.ClientError', startTime, endTime),
        getMessagesFromCloudWatch('Disconnect.Success', startTime, endTime)
    ]);
    
    return {
        successConnections,
        failedConnections,
        disconnections,
        totalConnectionAttempts: successConnections + failedConnections
    };
}

/**
 * 获取完整的每日消息统计
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Object>} - 完整的消息统计数据
 */
async function getDailyMessageStatistics(date) {
    const startTime = new Date(date);
    startTime.setHours(0, 0, 0, 0);
    
    const endTime = new Date(date);
    endTime.setHours(23, 59, 59, 999);
    
    console.log(`开始获取 ${date} 的消息统计...`);
    
    // 并行查询多个指标
    const metrics = [
        { name: 'PublishIn.Success', label: '入站成功消息' },
        { name: 'PublishIn.ClientError', label: '入站客户端错误' },
        { name: 'PublishIn.ServerError', label: '入站服务器错误' },
        { name: 'PublishOut.Success', label: '出站成功消息' },
        { name: 'PublishOut.ClientError', label: '出站客户端错误' },
        { name: 'PublishOut.ServerError', label: '出站服务器错误' },
        { name: 'Connect.Success', label: '成功连接数' },
        { name: 'Connect.ClientError', label: '连接客户端错误' },
        { name: 'Connect.ServerError', label: '连接服务器错误' },
        { name: 'Disconnect.Success', label: '成功断开连接数' },
        { name: 'Subscribe.Success', label: '成功订阅数' },
        { name: 'Unsubscribe.Success', label: '成功取消订阅数' }
    ];
    
    const results = await Promise.all(
        metrics.map(async (metric) => {
            try {
                const value = await getMessagesFromCloudWatch(metric.name, startTime, endTime);
                return {
                    metric: metric.name,
                    label: metric.label,
                    value: value
                };
            } catch (error) {
                console.error(`查询指标 ${metric.name} 失败:`, error);
                return {
                    metric: metric.name,
                    label: metric.label,
                    value: 0
                };
            }
        })
    );
    
    // 整理统计结果
    const statistics = {
        date: date,
        timestamp: new Date().toISOString(),
        inbound: {
            success: results.find(r => r.metric === 'PublishIn.Success')?.value || 0,
            clientError: results.find(r => r.metric === 'PublishIn.ClientError')?.value || 0,
            serverError: results.find(r => r.metric === 'PublishIn.ServerError')?.value || 0
        },
        outbound: {
            success: results.find(r => r.metric === 'PublishOut.Success')?.value || 0,
            clientError: results.find(r => r.metric === 'PublishOut.ClientError')?.value || 0,
            serverError: results.find(r => r.metric === 'PublishOut.ServerError')?.value || 0
        },
        connections: {
            success: results.find(r => r.metric === 'Connect.Success')?.value || 0,
            clientError: results.find(r => r.metric === 'Connect.ClientError')?.value || 0,
            serverError: results.find(r => r.metric === 'Connect.ServerError')?.value || 0,
            disconnects: results.find(r => r.metric === 'Disconnect.Success')?.value || 0
        },
        subscriptions: {
            subscribe: results.find(r => r.metric === 'Subscribe.Success')?.value || 0,
            unsubscribe: results.find(r => r.metric === 'Unsubscribe.Success')?.value || 0
        }
    };
    
    // 计算总计和成功率
    statistics.inbound.total = statistics.inbound.success + statistics.inbound.clientError + statistics.inbound.serverError;
    statistics.outbound.total = statistics.outbound.success + statistics.outbound.clientError + statistics.outbound.serverError;
    statistics.connections.total = statistics.connections.success + statistics.connections.clientError + statistics.connections.serverError;
    
    statistics.totalMessages = statistics.inbound.total + statistics.outbound.total;
    statistics.inbound.successRate = statistics.inbound.total > 0 ? 
        ((statistics.inbound.success / statistics.inbound.total) * 100).toFixed(2) : 0;
    statistics.outbound.successRate = statistics.outbound.total > 0 ? 
        ((statistics.outbound.success / statistics.outbound.total) * 100).toFixed(2) : 0;
    statistics.connections.successRate = statistics.connections.total > 0 ? 
        ((statistics.connections.success / statistics.connections.total) * 100).toFixed(2) : 0;
    
    console.log(`${date} 消息统计完成:`, {
        totalMessages: statistics.totalMessages,
        inboundMessages: statistics.inbound.total,
        outboundMessages: statistics.outbound.total,
        connections: statistics.connections.total
    });
    
    return statistics;
}

/**
 * 获取一天内每小时的消息分布
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Array>} - 24小时的消息分布数据
 */
async function getHourlyMessageBreakdown(date) {
    const hourlyData = [];
    
    console.log(`开始获取 ${date} 的小时级消息分布...`);
    
    for (let hour = 0; hour < 24; hour++) {
        const startTime = new Date(date);
        startTime.setHours(hour, 0, 0, 0);
        
        const endTime = new Date(date);
        endTime.setHours(hour, 59, 59, 999);
        
        const [inboundMessages, outboundMessages, connections] = await Promise.all([
            getMessagesFromCloudWatch('PublishIn.Success', startTime, endTime, 3600),
            getMessagesFromCloudWatch('PublishOut.Success', startTime, endTime, 3600),
            getMessagesFromCloudWatch('Connect.Success', startTime, endTime, 3600)
        ]);
        
        hourlyData.push({
            hour: hour,
            timeRange: `${hour.toString().padStart(2, '0')}:00-${hour.toString().padStart(2, '0')}:59`,
            inboundMessages: inboundMessages,
            outboundMessages: outboundMessages,
            totalMessages: inboundMessages + outboundMessages,
            connections: connections
        });
    }
    
    console.log(`${date} 小时级分布获取完成`);
    return hourlyData;
}

/**
 * 从S3获取昨天的消息统计数据
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Object>} - 昨天的消息统计数据
 */
async function getMessageStatisticsFromS3(date) {
    try {
        const year = date.split('-')[0];
        const month = date.split('-')[1];
        const key = `${year}/${month}/message-statistic-${date}.json`;
        
        const command = new GetObjectCommand({
            Bucket: S3_BUCKET,
            Key: `${S3_KEY_PREFIX}${key}`
        });
        
        const response = await s3Client.send(command);
        
        // 读取S3对象内容
        const stream = response.Body;
        if (stream instanceof Readable) {
            const chunks = [];
            for await (const chunk of stream) {
                chunks.push(chunk);
            }
            const data = Buffer.concat(chunks).toString('utf-8');
            return JSON.parse(data);
        } else {
            const data = await response.Body.transformToString();
            return JSON.parse(data);
        }
    } catch (error) {
        console.log(`从S3获取${date}消息统计数据失败: ${error.message}`);
        return {
            totalMessages: 0,
            inbound: { total: 0, success: 0 },
            outbound: { total: 0, success: 0 },
            connections: { total: 0, success: 0 }
        };
    }
}

/**
 * 将消息统计数据保存到S3
 * @param {Object} data - 要保存的消息统计数据
 */
async function saveMessageStatisticsToS3(data) {
    try {
        const date = data.date;
        const year = date.split('-')[0];
        const month = date.split('-')[1];
        const key = `${year}/${month}/message-statistic-${date}.json`;
        
        const command = new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: `${S3_KEY_PREFIX}${key}`,
            Body: JSON.stringify(data, null, 2),
            ContentType: "application/json"
        });
        
        await s3Client.send(command);
        console.log(`消息统计数据已保存到S3: ${key}`);
    } catch (error) {
        console.error(`保存消息统计数据到S3失败: ${error.message}`);
        throw error;
    }
}

/**
 * 获取消息统计趋势（多天对比）
 * @param {number} days - 查询天数
 * @returns {Promise<Array>} - 多天的消息统计趋势
 */
async function getMessageStatisticsTrend(days = 7) {
    const trendData = [];
    const today = new Date();
    
    for (let i = days - 1; i >= 0; i--) {
        const date = new Date(today);
        date.setDate(date.getDate() - i);
        const dateStr = date.toISOString().split('T')[0];
        
        try {
            // 优先从S3获取历史数据
            let dayStats = await getMessageStatisticsFromS3(dateStr);
            
            // 如果S3没有数据且是今天，则从CloudWatch获取
            if (dayStats.totalMessages === 0 && i === 0) {
                dayStats = await getDailyMessageStatistics(dateStr);
            }
            
            trendData.push({
                date: dateStr,
                totalMessages: dayStats.totalMessages || 0,
                inboundMessages: dayStats.inbound?.total || 0,
                outboundMessages: dayStats.outbound?.total || 0,
                connections: dayStats.connections?.total || 0,
                inboundSuccessRate: dayStats.inbound?.successRate || 0,
                outboundSuccessRate: dayStats.outbound?.successRate || 0
            });
        } catch (error) {
            console.error(`获取${dateStr}消息统计失败:`, error);
            trendData.push({
                date: dateStr,
                totalMessages: 0,
                inboundMessages: 0,
                outboundMessages: 0,
                connections: 0,
                inboundSuccessRate: 0,
                outboundSuccessRate: 0
            });
        }
    }
    
    return trendData;
}

/**
 * 格式化消息统计为钉钉消息
 * @param {Object} statistics - 消息统计数据
 * @param {Object} comparison - 对比数据（可选）
 * @returns {Object} - 格式化的钉钉消息
 */
function formatIotMsgStatisticsMessage(statistics) {
    const now = new Date().toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
    });

    let markdown = `
# AWS IoT Core 消息统计日报 - ${statistics.date}
> 生成时间: ${now}

## 📨 消息总览
- 消息总数: **${statistics.totalMessages.toLocaleString()}**
- 入站消息: **${statistics.inbound.total.toLocaleString()}** (成功率: ${statistics.inbound.successRate}%)
- 出站消息: **${statistics.outbound.total.toLocaleString()}** (成功率: ${statistics.outbound.successRate}%)

## 🔗 连接统计
- 连接总数: **${statistics.connections.total.toLocaleString()}** (成功率: ${statistics.connections.successRate}%)
- 成功连接: **${statistics.connections.success.toLocaleString()}**
- 断开连接: **${statistics.connections.disconnects.toLocaleString()}**

## 📊 订阅统计
- 订阅操作: **${statistics.subscriptions.subscribe.toLocaleString()}**
- 取消订阅: **${statistics.subscriptions.unsubscribe.toLocaleString()}**
`;

    markdown += `\n---\n*数据来源: CloudWatch Metrics*`;

    return {
        msgtype: "markdown",
        markdown: {
            title: `AWS IoT Core 消息统计日报 - ${statistics.date}`,
            text: markdown,
        },
    };
}

/**
 * 获取AWS IoT消息统计的主函数
 * @param {string} date - 日期 (YYYY-MM-DD)，默认为今天
 * @returns {Promise<Object>} - 消息统计数据和钉钉消息
 */
async function getAWSIotMsgStatistic(date = null) {
    try {
        const targetDate = date || new Date().toISOString().split('T')[0];
        console.log(`开始获取AWS IoT消息统计数据: ${targetDate}`);
        
        // 获取今日消息统计
        const todayStats = await getDailyMessageStatistics(targetDate);
        
        // 获取昨天数据用于对比
        // const yesterday = new Date(targetDate);
        // yesterday.setDate(yesterday.getDate() - 1);
        // const yesterdayStr = yesterday.toISOString().split('T')[0];
        // const yesterdayStats = await getMessageStatisticsFromS3(yesterdayStr);
        
        // 保存今日统计数据到S3
        await saveMessageStatisticsToS3(todayStats);

        return todayStats;

        
        console.log('AWS IoT消息统计数据获取完成');
        
    } catch (error) {
        console.error('获取AWS IoT消息统计数据失败:', error);
        
        const errorMessage = {
            msgtype: "text",
            text: {
                content: `AWS IoT消息统计数据获取失败: ${error.message}`
            }
        };
        
        return {
            statistics: null,
            dingTalkMessage: errorMessage,
            error: error.message
        };
    }
}

export {
    getAWSIotMsgStatistic,
    formatIotMsgStatisticsMessage
};
