

import { CloudWatchClient, GetMetricStatisticsCommand, GetInsightRuleReportCommand } from "@aws-sdk/client-cloudwatch";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { IoTClient, ListThingsCommand, DescribeThingCommand } from "@aws-sdk/client-iot";
import { Readable } from 'stream';

// 初始化客户端
const cloudwatchClient = new CloudWatchClient({ region: "us-east-1" });
const s3Client = new S3Client({ region: "us-east-1" });
const iotClient = new IoTClient({ region: "us-east-1" });

// 配置
const S3_BUCKET = "govee-logs";
const S3_KEY_PREFIX = "iotcore-logs/";

/**
 * 从CloudWatch获取指定错误指标的统计数据
 * @param {string} metricName - 指标名称
 * @param {Date} startTime - 开始时间
 * @param {Date} endTime - 结束时间
 * @param {Array} dimensions - 维度（可选）
 * @returns {Promise<number>} - 错误数量
 */
async function getErrorMetricFromCloudWatch(metricName, startTime, endTime, dimensions = []) {
    try {
        const params = {
            Namespace: 'AWS/IoT',
            MetricName: metricName,
            StartTime: startTime,
            EndTime: endTime,
            Period: 86400,
            Statistics: ['Sum'],
            Dimensions: dimensions
        };

        const response = await cloudwatchClient.send(new GetMetricStatisticsCommand(params));
        const totalErrors = response.Datapoints.length > 0 ? response.Datapoints[0].Sum : 0;
        
        console.log(`CloudWatch ${metricName}: ${totalErrors.toLocaleString()}`);
        return totalErrors;
    } catch (error) {
        console.error(`获取CloudWatch错误指标 ${metricName} 失败:`, error);
        return 0;
    }
}

/**
 * 获取Top 10重复客户端ID（应用类型）
 * @param {Date} startTime - 开始时间
 * @param {Date} endTime - 结束时间
 * @returns {Promise<Object>} - 重复客户端统计
 */
async function getTop10DuplicateAppClients(startTime, endTime) {
    try {
        const params = {
            RuleName: 'iot-duplicateclientid-account',
            StartTime: startTime,
            EndTime: endTime,
            Period: 86400,
            MaxContributorCount: 10
        };

        const response = await cloudwatchClient.send(new GetInsightRuleReportCommand(params));

        console.log(`应用重复客户端 - 总事件数: ${response.AggregateValue}`);
        console.log(`应用重复客户端 - 唯一客户端数: ${response.ApproximateUniqueCount}`);

        const duplicateClients = response.Contributors?.map((contributor, index) => ({
            rank: index + 1,
            clientId: contributor.Keys[0],
            sourceIp: contributor.Keys[1] || 'Unknown',
            duplicateCount: contributor.ApproximateAggregateValue,
            percentage: ((contributor.ApproximateAggregateValue / response.AggregateValue) * 100).toFixed(2)
        })) || [];

        return {
            totalEvents: response.AggregateValue || 0,
            uniqueClients: response.ApproximateUniqueCount || 0,
            duplicateClients: duplicateClients,
            type: 'application'
        };
    } catch (error) {
        console.error('获取应用重复客户端失败:', error);
        return {
            totalEvents: 0,
            uniqueClients: 0,
            duplicateClients: [],
            type: 'application',
            error: error.message
        };
    }
}

/**
 * 获取Top 10重复客户端ID（设备类型）
 * @param {Date} startTime - 开始时间
 * @param {Date} endTime - 结束时间
 * @returns {Promise<Object>} - 重复设备客户端统计
 */
async function getTop10DuplicateDeviceClients(startTime, endTime) {
    try {
        const params = {
            RuleName: 'iot-duplicateclientid-device',
            StartTime: startTime,
            EndTime: endTime,
            Period: 86400,
            MaxContributorCount: 10
        };

        const response = await cloudwatchClient.send(new GetInsightRuleReportCommand(params));

        console.log(`设备重复客户端 - 总事件数: ${response.AggregateValue}`);
        console.log(`设备重复客户端 - 唯一客户端数: ${response.ApproximateUniqueCount}`);

        const duplicateClients = response.Contributors?.map((contributor, index) => ({
            rank: index + 1,
            clientId: contributor.Keys[0],
            sourceIp: contributor.Keys[1] || 'Unknown',
            duplicateCount: contributor.ApproximateAggregateValue,
            percentage: ((contributor.ApproximateAggregateValue / response.AggregateValue) * 100).toFixed(2)
        })) || [];

        return {
            totalEvents: response.AggregateValue || 0,
            uniqueClients: response.ApproximateUniqueCount || 0,
            duplicateClients: duplicateClients,
            type: 'device'
        };
    } catch (error) {
        console.error('获取设备重复客户端失败:', error);
        return {
            totalEvents: 0,
            uniqueClients: 0,
            duplicateClients: [],
            type: 'device',
            error: error.message
        };
    }
}

/**
 * 获取每日IoT错误统计
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Object>} - 完整的错误统计数据
 */
async function getDailyIoTErrorStatistics(date) {
    const startTime = new Date(date);
    startTime.setHours(0, 0, 0, 0);
    
    const endTime = new Date(date);
    endTime.setHours(23, 59, 59, 999);
    
    console.log(`开始获取 ${date} 的IoT错误统计...`);
    
    // 并行查询多个错误指标
    const errorMetrics = [
        { name: 'Connect.AuthError', label: '认证错误' },
        { name: 'Connect.ClientError', label: '连接客户端错误' },
        { name: 'Connect.ServerError', label: '连接服务器错误' },
        { name: 'Connect.Throttle', label: '连接限流' },
        { name: 'PublishIn.AuthError', label: '入站认证错误' },
        { name: 'PublishIn.ClientError', label: '入站客户端错误' },
        { name: 'PublishIn.ServerError', label: '入站服务器错误' },
        { name: 'PublishOut.AuthError', label: '出站认证错误' },
        { name: 'PublishOut.ClientError', label: '出站客户端错误' },
        { name: 'PublishOut.ServerError', label: '出站服务器错误' },
        { name: 'Subscribe.AuthError', label: '订阅认证错误' },
        { name: 'Subscribe.ClientError', label: '订阅客户端错误' },
        { name: 'Subscribe.ServerError', label: '订阅服务器错误' },
        { name: 'Subscribe.Throttle', label: '订阅限流' }
    ];
    
    // 并行查询错误指标和重复客户端
    const [errorResults, duplicateAppClients, duplicateDeviceClients] = await Promise.all([
        Promise.all(
            errorMetrics.map(async (metric) => {
                try {
                    const value = await getErrorMetricFromCloudWatch(metric.name, startTime, endTime);
                    return {
                        metric: metric.name,
                        label: metric.label,
                        value: value
                    };
                } catch (error) {
                    console.error(`查询错误指标 ${metric.name} 失败:`, error);
                    return {
                        metric: metric.name,
                        label: metric.label,
                        value: 0
                    };
                }
            })
        ),
        getTop10DuplicateAppClients(startTime, endTime),
        getTop10DuplicateDeviceClients(startTime, endTime)
    ]);
    
    // 整理错误统计结果
    const statistics = {
        date: date,
        timestamp: new Date().toISOString(),
        connectionErrors: {
            authError: errorResults.find(r => r.metric === 'Connect.AuthError')?.value || 0,
            clientError: errorResults.find(r => r.metric === 'Connect.ClientError')?.value || 0,
            serverError: errorResults.find(r => r.metric === 'Connect.ServerError')?.value || 0,
            throttle: errorResults.find(r => r.metric === 'Connect.Throttle')?.value || 0
        },
        publishInErrors: {
            authError: errorResults.find(r => r.metric === 'PublishIn.AuthError')?.value || 0,
            clientError: errorResults.find(r => r.metric === 'PublishIn.ClientError')?.value || 0,
            serverError: errorResults.find(r => r.metric === 'PublishIn.ServerError')?.value || 0
        },
        publishOutErrors: {
            authError: errorResults.find(r => r.metric === 'PublishOut.AuthError')?.value || 0,
            clientError: errorResults.find(r => r.metric === 'PublishOut.ClientError')?.value || 0,
            serverError: errorResults.find(r => r.metric === 'PublishOut.ServerError')?.value || 0
        },
        subscribeErrors: {
            authError: errorResults.find(r => r.metric === 'Subscribe.AuthError')?.value || 0,
            clientError: errorResults.find(r => r.metric === 'Subscribe.ClientError')?.value || 0,
            serverError: errorResults.find(r => r.metric === 'Subscribe.ServerError')?.value || 0,
            throttle: errorResults.find(r => r.metric === 'Subscribe.Throttle')?.value || 0
        },
        duplicateClients: {
            applications: duplicateAppClients,
            devices: duplicateDeviceClients
        }
    };
    
    // 计算总计
    statistics.connectionErrors.total = Object.values(statistics.connectionErrors).reduce((sum, val) => 
        typeof val === 'number' ? sum + val : sum, 0);
    statistics.publishInErrors.total = Object.values(statistics.publishInErrors).reduce((sum, val) => 
        typeof val === 'number' ? sum + val : sum, 0);
    statistics.publishOutErrors.total = Object.values(statistics.publishOutErrors).reduce((sum, val) => 
        typeof val === 'number' ? sum + val : sum, 0);
    statistics.subscribeErrors.total = Object.values(statistics.subscribeErrors).reduce((sum, val) => 
        typeof val === 'number' ? sum + val : sum, 0);
    
    statistics.totalErrors = statistics.connectionErrors.total + 
                           statistics.publishInErrors.total + 
                           statistics.publishOutErrors.total + 
                           statistics.subscribeErrors.total;
    
    console.log(`${date} IoT错误统计完成:`, {
        totalErrors: statistics.totalErrors,
        connectionErrors: statistics.connectionErrors.total,
        publishErrors: statistics.publishInErrors.total + statistics.publishOutErrors.total,
        subscribeErrors: statistics.subscribeErrors.total,
        duplicateAppClients: statistics.duplicateClients.applications.totalEvents,
        duplicateDeviceClients: statistics.duplicateClients.devices.totalEvents
    });
    
    return statistics;
}

/**
 * 将错误统计数据保存到S3
 * @param {Object} data - 要保存的错误统计数据
 */
async function saveErrorStatisticsToS3(data) {
    try {
        const date = data.date;
        const year = date.split('-')[0];
        const month = date.split('-')[1];
        const key = `${year}/${month}/error-statistic-${date}.json`;
        
        const command = new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: `${S3_KEY_PREFIX}${key}`,
            Body: JSON.stringify(data, null, 2),
            ContentType: "application/json"
        });
        
        await s3Client.send(command);
        console.log(`错误统计数据已保存到S3: ${key}`);
    } catch (error) {
        console.error(`保存错误统计数据到S3失败: ${error.message}`);
        throw error;
    }
}

/**
 * 从S3获取错误统计数据
 * @param {string} date - 日期 (YYYY-MM-DD)
 * @returns {Promise<Object>} - 错误统计数据
 */
async function getErrorStatisticsFromS3(date) {
    try {
        const year = date.split('-')[0];
        const month = date.split('-')[1];
        const key = `${year}/${month}/error-statistic-${date}.json`;
        
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
        console.log(`从S3获取${date}错误统计数据失败: ${error.message}`);
        return {
            totalErrors: 0,
            connectionErrors: { total: 0 },
            publishInErrors: { total: 0 },
            publishOutErrors: { total: 0 },
            subscribeErrors: { total: 0 },
            duplicateClients: {
                applications: { totalEvents: 0, duplicateClients: [] },
                devices: { totalEvents: 0, duplicateClients: [] }
            }
        };
    }
}

/**
 * 格式化IoT错误统计为钉钉消息
 * @param {Object} statistics - 错误统计数据
 * @param {Object} comparison - 对比数据（可选）
 * @returns {Object} - 格式化的钉钉消息
 */
function formatIoTErrorStatisticsMessage(statistics ) {
    const now = new Date().toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
    });

    let markdown = `
# AWS IoT Core 错误统计日报 - ${statistics.date}
> 生成时间: ${now}

## 🚨 错误总览
- 错误总数: **${statistics.totalErrors.toLocaleString()}**
- 连接错误: **${statistics.connectionErrors.total.toLocaleString()}**
- 发布错误: **${(statistics.publishInErrors.total + statistics.publishOutErrors.total).toLocaleString()}**
- 订阅错误: **${statistics.subscribeErrors.total.toLocaleString()}**

## 🔌 连接错误详情
- 认证错误: **${statistics.connectionErrors.authError.toLocaleString()}**
- 客户端错误: **${statistics.connectionErrors.clientError.toLocaleString()}**
- 服务器错误: **${statistics.connectionErrors.serverError.toLocaleString()}**
- 限流错误: **${statistics.connectionErrors.throttle.toLocaleString()}**

## 📤 发布错误详情
**入站消息错误:**
- 认证错误: **${statistics.publishInErrors.authError.toLocaleString()}**
- 客户端错误: **${statistics.publishInErrors.clientError.toLocaleString()}**
- 服务器错误: **${statistics.publishInErrors.serverError.toLocaleString()}**

**出站消息错误:**
- 认证错误: **${statistics.publishOutErrors.authError.toLocaleString()}**
- 客户端错误: **${statistics.publishOutErrors.clientError.toLocaleString()}**
- 服务器错误: **${statistics.publishOutErrors.serverError.toLocaleString()}**

## 📋 订阅错误详情
- 认证错误: **${statistics.subscribeErrors.authError.toLocaleString()}**
- 客户端错误: **${statistics.subscribeErrors.clientError.toLocaleString()}**
- 服务器错误: **${statistics.subscribeErrors.serverError.toLocaleString()}**
- 限流错误: **${statistics.subscribeErrors.throttle.toLocaleString()}**
`;

    // 添加重复客户端信息
    if (statistics.duplicateClients.applications.totalEvents > 0 || 
        statistics.duplicateClients.devices.totalEvents > 0) {
        markdown += `
## 🔄 重复客户端ID统计
**应用类型重复:** ${statistics.duplicateClients.applications.totalEvents.toLocaleString()} 次
**设备类型重复:** ${statistics.duplicateClients.devices.totalEvents.toLocaleString()} 次
`;

        // Top 5 重复应用客户端
        if (statistics.duplicateClients.applications.duplicateClients.length > 0) {
            markdown += `\n**Top 5 重复应用客户端:**\n`;
            statistics.duplicateClients.applications.duplicateClients.slice(0, 5).forEach(client => {
                markdown += `- ${client.clientId}: **${client.duplicateCount}** 次 (${client.percentage}%)\n`;
            });
        }

        // Top 5 重复设备客户端
        if (statistics.duplicateClients.devices.duplicateClients.length > 0) {
            markdown += `\n**Top 5 重复设备客户端:**\n`;
            statistics.duplicateClients.devices.duplicateClients.slice(0, 5).forEach(client => {
                markdown += `- ${client.clientId}: **${client.duplicateCount}** 次 (${client.percentage}%)\n`;
            });
        }
    }

    markdown += `\n---\n*数据来源: CloudWatch Metrics & Insights*`;

    return {
        msgtype: "markdown",
        markdown: {
            title: `AWS IoT Core 错误统计日报 - ${statistics.date}`,
            text: markdown,
        },
    };
}

/**
 * 获取AWS IoT错误统计的主函数
 * @param {string} date - 日期 (YYYY-MM-DD)，默认为今天
 * @returns {Promise<Object>} - 错误统计数据和钉钉消息
 */
async function getAWSIoTErrorStatistic(date = null) {
    try {
        const targetDate = date || new Date().toISOString().split('T')[0];
        console.log(`开始获取AWS IoT错误统计数据: ${targetDate}`);
        
        // 获取今日错误统计
        const todayStats = await getDailyIoTErrorStatistics(targetDate);
        
        // 保存今日统计数据到S3
        await saveErrorStatisticsToS3(todayStats);

        // 格式化钉钉消息
        // const dingTalkMessage = formatIoTErrorStatisticsMessage(todayStats);
        console.log('AWS IoT错误统计数据获取完成');
        return targetDate;

        

    } catch (error) {
        console.error('获取AWS IoT错误统计数据失败:', error);
        
        const errorMessage = {
            msgtype: "text",
            text: {
                content: `AWS IoT错误统计数据获取失败: ${error.message}`
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
    getAWSIoTErrorStatistic,
    formatIoTErrorStatisticsMessage,
    getTop10DuplicateAppClients,
    getTop10DuplicateDeviceClients,
    getDailyIoTErrorStatistics
};
