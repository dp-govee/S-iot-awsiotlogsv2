

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
        // 确保时间范围至少为 Period 的长度，且Period必须是60的倍数
        const timeDiffSeconds = Math.floor((endTime - startTime) / 1000);
        let period = Math.min(86400, Math.max(3600, timeDiffSeconds)); // 最小1小时，最大24小时
        
        // 确保period是60的倍数
        period = Math.floor(period / 60) * 60;
        if (period < 60) period = 60; // 最小60秒
        
        const params = {
            RuleName: 'iot-duplicateclientid-account',
            StartTime: startTime,
            EndTime: endTime,
            Period: period,
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
        // 确保时间范围至少为 Period 的长度，且Period必须是60的倍数
        const timeDiffSeconds = Math.floor((endTime - startTime) / 1000);
        let period = Math.min(86400, Math.max(3600, timeDiffSeconds)); // 最小1小时，最大24小时
        
        // 确保period是60的倍数
        period = Math.floor(period / 60) * 60;
        if (period < 60) period = 60; // 最小60秒
        
        const params = {
            RuleName: 'iot-duplicateclientid-device',
            StartTime: startTime,
            EndTime: endTime,
            Period: period,
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
 * 获取Top 10授权失败客户端ID（应用类型）
 * @param {Date} startTime - 开始时间
 * @param {Date} endTime - 结束时间
 * @returns {Promise<Object>} - 授权失败客户端统计
 */
async function getTop10AuthFailureAPPClients(startTime, endTime) {
    try {
        // 确保时间范围至少为 Period 的长度，且Period必须是60的倍数
        const timeDiffSeconds = Math.floor((endTime - startTime) / 1000);
        let period = Math.min(86400, Math.max(3600, timeDiffSeconds)); // 最小1小时，最大24小时
        
        // 确保period是60的倍数
        period = Math.floor(period / 60) * 60;
        if (period < 60) period = 60; // 最小60秒

        const params = {
            RuleName: 'iot-authorizationfailure-app',
            StartTime: startTime,
            EndTime: endTime,
            Period: period,
            MaxContributorCount: 10
        };

        const response = await cloudwatchClient.send(new GetInsightRuleReportCommand(params));

        console.log(`应用授权失败 - 总事件数: ${response.AggregateValue}`);
        console.log(`应用授权失败 - 唯一客户端数: ${response.ApproximateUniqueCount}`);

        const authFailureClients = response.Contributors?.map((contributor, index) => ({
            rank: index + 1,
            clientId: contributor.Keys[0],
            sourceIp: contributor.Keys[1] || 'Unknown',
            duplicateCount: contributor.ApproximateAggregateValue,
            percentage: ((contributor.ApproximateAggregateValue / response.AggregateValue) * 100).toFixed(2)
        })) || [];

        return {
            totalEvents: response.AggregateValue || 0,
            uniqueClients: response.ApproximateUniqueCount || 0,
            authFailureClients: authFailureClients,
            type: 'application'
        };
    } catch (error) {
        console.error('获取应用授权失败客户端失败:', error);
        return {
            totalEvents: 0,
            uniqueClients: 0,
            authFailureClients: [],
            type: 'application',
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

    // 并行查询错误指标和重复客户端
    const [ duplicateAppClients, duplicateDeviceClients, authFailureAppClients] = await Promise.all([
        getTop10DuplicateAppClients(startTime, endTime),
        getTop10DuplicateDeviceClients(startTime, endTime),
        getTop10AuthFailureAPPClients(startTime,endTime)
    ]);

    // 整理错误统计结果
    const statistics = {
        date: date,
        timestamp: new Date().toISOString(),
        duplicateAppClients: duplicateAppClients,
        duplicateDeviceClients: duplicateDeviceClients,
        authFailureAppClients:authFailureAppClients
    };


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
function formatIoTErrorStatisticsMessage(statistics, comparison = null) {
    // 数据验证
    if (!statistics || !statistics.date) {
        return {
            msgtype: "text",
            text: {
                content: "❌ 错误：统计数据格式不正确或缺少日期信息"
            }
        };
    }

    const now = new Date().toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
    });

    let markdown = `# 📊 AWS IoT Core 错误统计日报
## ${statistics.date}
> 🕐 生成时间: ${now}

`;

    // 计算总体统计
    const totalDuplicateApp = statistics.duplicateAppClients?.totalEvents || 0;
    const totalDuplicateDevice = statistics.duplicateDeviceClients?.totalEvents || 0;
    const totalAuthFailure = statistics.authFailureAppClients?.totalEvents || 0;
    const totalErrors = totalDuplicateApp + totalDuplicateDevice + totalAuthFailure;

    // 总览部分
    markdown += `## 📈 总览统计
| 错误类型 | 事件数量 | 占比 |
|---------|---------|------|
| 🔄 应用重复客户端 | ${totalDuplicateApp.toLocaleString()} | ${totalErrors > 0 ? ((totalDuplicateApp / totalErrors) * 100).toFixed(1) : '0'}% |
| 🔄 设备重复客户端 | ${totalDuplicateDevice.toLocaleString()} | ${totalErrors > 0 ? ((totalDuplicateDevice / totalErrors) * 100).toFixed(1) : '0'}% |
| 🚫 授权失败 | ${totalAuthFailure.toLocaleString()} | ${totalErrors > 0 ? ((totalAuthFailure / totalErrors) * 100).toFixed(1) : '0'}% |
| **总计** | **${totalErrors.toLocaleString()}** | **100%** |

`;

    // 重复客户端ID统计 - 应用类型
    if (statistics.duplicateAppClients) {
        const appStats = statistics.duplicateAppClients;
        markdown += `## 🔄 重复客户端ID - 应用类型 (AP/)
**📊 统计概览:**
- 总事件数: **${(appStats.totalEvents || 0).toLocaleString()}** 次
- 唯一客户端: **${(appStats.uniqueClients || 0).toLocaleString()}** 个
- 平均每客户端: **${appStats.uniqueClients > 0 ? Math.round(appStats.totalEvents / appStats.uniqueClients) : 0}** 次

`;

        // Top 5 重复应用客户端
        if (appStats.duplicateClients && appStats.duplicateClients.length > 0) {
            markdown += `**🏆 Top 5 重复应用客户端:**
`;
            appStats.duplicateClients.slice(0, 5).forEach((client, index) => {
                const clientIdShort = client.clientId.length > 50 ? 
                    client.clientId.substring(0, 47) + '...' : client.clientId;
                markdown += `${index + 1}. \`${clientIdShort}\`
   - 📍 IP: ${client.sourceIp}
   - 🔢 重复: **${client.duplicateCount.toLocaleString()}** 次 (${client.percentage}%)
`;
            });
        } else {
            markdown += `✅ **无重复应用客户端记录**
`;
        }

        if (appStats.error) {
            markdown += `⚠️ *数据获取异常: ${appStats.error}*
`;
        }
    }

    // 重复客户端ID统计 - 设备类型
    if (statistics.duplicateDeviceClients) {
        const deviceStats = statistics.duplicateDeviceClients;
        markdown += `
## 🔄 重复客户端ID - 设备类型 (GD/)
**📊 统计概览:**
- 总事件数: **${(deviceStats.totalEvents || 0).toLocaleString()}** 次
- 唯一客户端: **${(deviceStats.uniqueClients || 0).toLocaleString()}** 个
- 平均每客户端: **${deviceStats.uniqueClients > 0 ? Math.round(deviceStats.totalEvents / deviceStats.uniqueClients) : 0}** 次

`;

        // Top 5 重复设备客户端
        if (deviceStats.duplicateClients && deviceStats.duplicateClients.length > 0) {
            markdown += `**🏆 Top 5 重复设备客户端:**
`;
            deviceStats.duplicateClients.slice(0, 5).forEach((client, index) => {
                const clientIdShort = client.clientId.length > 50 ? 
                    client.clientId.substring(0, 47) + '...' : client.clientId;
                markdown += `${index + 1}. \`${clientIdShort}\`
   - 📍 IP: ${client.sourceIp}
   - 🔢 重复: **${client.duplicateCount.toLocaleString()}** 次 (${client.percentage}%)
`;
            });
        } else {
            markdown += `✅ **无重复设备客户端记录**
`;
        }

        if (deviceStats.error) {
            markdown += `⚠️ *数据获取异常: ${deviceStats.error}*
`;
        }
    }

    // 授权失败统计 - 应用类型
    if (statistics.authFailureAppClients) {
        const authStats = statistics.authFailureAppClients;
        markdown += `
## 🚫 授权失败统计 - 应用类型
**📊 统计概览:**
- 总失败数: **${(authStats.totalEvents || 0).toLocaleString()}** 次
- 唯一客户端: **${(authStats.uniqueClients || 0).toLocaleString()}** 个
- 平均每客户端: **${authStats.uniqueClients > 0 ? Math.round(authStats.totalEvents / authStats.uniqueClients) : 0}** 次

`;

        // Top 5 授权失败应用客户端
        if (authStats.authFailureAppClients && authStats.authFailureAppClients.length > 0) {
            markdown += `**🏆 Top 5 授权失败客户端:**
`;
            authStats.authFailureAppClients.slice(0, 5).forEach((client, index) => {
                const clientIdShort = client.clientId.length > 50 ? 
                    client.clientId.substring(0, 47) + '...' : client.clientId;
                markdown += `${index + 1}. \`${clientIdShort}\`
   - 📍 IP: ${client.sourceIp}
   - ❌ 失败: **${client.duplicateCount.toLocaleString()}** 次 (${client.percentage}%)
`;
            });
        } else {
            markdown += `✅ **无授权失败记录**
`;
        }

        if (authStats.error) {
            markdown += `⚠️ *数据获取异常: ${authStats.error}*
`;
        }
    }

    // 添加对比数据（如果提供）
    if (comparison) {
        markdown += `
## 📊 日环比分析
`;
        
        const comparisons = [
            {
                name: '应用重复客户端',
                today: totalDuplicateApp,
                yesterday: comparison.duplicateAppClients?.totalEvents || 0,
                icon: '🔄'
            },
            {
                name: '设备重复客户端',
                today: totalDuplicateDevice,
                yesterday: comparison.duplicateDeviceClients?.totalEvents || 0,
                icon: '🔄'
            },
            {
                name: '授权失败',
                today: totalAuthFailure,
                yesterday: comparison.authFailureAppClients?.totalEvents || 0,
                icon: '🚫'
            }
        ];

        comparisons.forEach(item => {
            const change = item.today - item.yesterday;
            const changePercent = item.yesterday > 0 ? ((change / item.yesterday) * 100).toFixed(1) : 'N/A';
            
            let trend = '';
            if (change > 0) {
                trend = `📈 +${change.toLocaleString()} (+${changePercent}%)`;
            } else if (change < 0) {
                trend = `📉 ${change.toLocaleString()} (${changePercent}%)`;
            } else {
                trend = `➡️ 无变化`;
            }
            
            markdown += `${item.icon} **${item.name}:** ${item.today.toLocaleString()} 次 ${trend}
`;
        });
    }

    // 智能分析和建议
    markdown += `
## 🔍 智能分析与建议
`;

    if (totalErrors === 0) {
        markdown += `✅ **系统状态良好**
- 今日无错误事件记录
- 所有IoT客户端连接正常
- 建议继续保持当前配置

`;
    } else {
        // 错误严重程度分析
        let severity = '正常';
        let severityIcon = '✅';
        
        if (totalErrors > 50000) {
            severity = '严重';
            severityIcon = '🔴';
        } else if (totalErrors > 10000) {
            severity = '警告';
            severityIcon = '🟡';
        } else if (totalErrors > 1000) {
            severity = '注意';
            severityIcon = '🟠';
        }

        markdown += `${severityIcon} **错误严重程度:** ${severity}

`;
    }

    // 数据源信息
    markdown += `
---
📋 **报告信息**
- 数据来源: AWS CloudWatch Contributor Insights
- 监控规则: iot-duplicateclientid-account, iot-duplicateclientid-device, iot-authorizationfailure-app
- 日志组: AWSIotLogsV2
- 生成系统: AWS IoT Core 监控系统`;

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
 * @param {boolean} includeComparison - 是否包含对比数据，默认为true
 * @returns {Promise<Object>} - 错误统计数据和钉钉消息
 */
async function getAWSIoTErrorStatistic(date = null, includeComparison = true) {
    try {
        const targetDate = date || new Date().toISOString().split('T')[0];
        console.log(`开始获取AWS IoT错误统计数据: ${targetDate}`);
        
        // 获取今日错误统计
        const todayStats = await getDailyIoTErrorStatistics(targetDate);
        
        // 保存今日统计数据到S3
        await saveErrorStatisticsToS3(todayStats);

        let comparison = null;
        
        // 获取昨日数据用于对比（如果需要）
        if (includeComparison) {
            try {
                const yesterday = new Date(targetDate);
                yesterday.setDate(yesterday.getDate() - 1);
                const yesterdayDate = yesterday.toISOString().split('T')[0];
                
                console.log(`获取对比数据: ${yesterdayDate}`);
                comparison = await getErrorStatisticsFromS3(yesterdayDate);
                
                // 如果S3中没有昨日数据，尝试实时获取
                if (!comparison || (!comparison.duplicateAppClients && !comparison.duplicateDeviceClients && !comparison.authFailureAppClients)) {
                    console.log(`S3中无昨日数据，尝试实时获取: ${yesterdayDate}`);
                    comparison = await getDailyIoTErrorStatistics(yesterdayDate);
                }
            } catch (error) {
                console.log(`获取昨日对比数据失败: ${error.message}`);
                // 继续执行，不影响主要功能
            }
        }

        // 格式化钉钉消息
        const dingTalkMessage = formatIoTErrorStatisticsMessage(todayStats, comparison);
        
        console.log('AWS IoT错误统计数据获取完成');
        
        return {
            date: targetDate,
            statistics: todayStats,
            comparison: comparison,
            dingTalkMessage: dingTalkMessage,
            success: true
        };

    } catch (error) {
        console.error('获取AWS IoT错误统计数据失败:', error);
        
        const errorMessage = {
            msgtype: "text",
            text: {
                content: `❌ AWS IoT错误统计数据获取失败

**错误信息:** ${error.message}
**时间:** ${new Date().toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })}
**日期:** ${date || '今日'}

请检查以下项目：
- CloudWatch权限配置
- 网络连接状态
- Contributor Insights规则状态
- 日志组访问权限

如问题持续，请联系系统管理员。`
            }
        };
        
        return {
            date: date || new Date().toISOString().split('T')[0],
            statistics: null,
            comparison: null,
            dingTalkMessage: errorMessage,
            success: false,
            error: error.message
        };
    }
}

// console.log(getAWSIoTErrorStatistic())

export {
    getAWSIoTErrorStatistic,
    formatIoTErrorStatisticsMessage,
    getTop10DuplicateAppClients,
    getTop10DuplicateDeviceClients,
    getDailyIoTErrorStatistics,
    getTop10AuthFailureAPPClients
};
