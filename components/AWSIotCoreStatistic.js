import {IoTClient, ListThingsCommand} from "@aws-sdk/client-iot";
import {S3Client, GetObjectCommand, PutObjectCommand} from "@aws-sdk/client-s3";
import {DynamoDBClient, GetItemCommand, QueryCommand} from "@aws-sdk/client-dynamodb";
import {
    RedshiftDataClient,
    ExecuteStatementCommand,
    DescribeStatementCommand,
    GetStatementResultCommand
} from "@aws-sdk/client-redshift-data";
import {Readable} from 'stream';

// 初始化客户端
const iotClient = new IoTClient({region: "us-east-1"});
const s3Client = new S3Client({region: "us-east-1"});
const dynamoClient = new DynamoDBClient({region: "us-east-1"});
const redshiftDataClient = new RedshiftDataClient({region: "us-east-1"});

// 配置
const S3_BUCKET = "govee-logs";
const S3_KEY_PREFIX = "iotcore-logs/";
const DYNAMO_COUNTER_TABLE = "IoTDeviceCounters";
const DYNAMO_INCREMENT_TABLE = "IoTDailyIncrements";
const REDSHIFT_CLUSTER = "pro-redshift-cluster-1";
const REDSHIFT_DATABASE = "pro";
const REDSHIFT_DATABASE_USER = "awsuser";

/**
 * 从S3获取昨天的统计数据
 * @returns {Promise<Object>} - 昨天的统计数据
 */
async function getYesterdayStatisticsFromS3() {
    try {
        // 计算昨天的日期
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const yesterdayStr = yesterday.toISOString().split('T')[0];
        const year = yesterdayStr.split('-')[0];
        const month = yesterdayStr.split('-')[1];

        const key = `${year}/${month}/daily-statistic-${yesterdayStr}.json`;
        console.log('执行从s3查询昨日数据key:'+key);
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
            console.log('从s3查询昨日数据data:'+data);
            return JSON.parse(data);
        } else {
            const data = await response.Body.transformToString();
            console.log('_从s3查询昨日数据data:'+data);
            return JSON.parse(data);
        }
    } catch (error) {
        console.log(`从S3获取昨天统计数据失败: ${error.message}`);
        // 返回默认值，避免程序中断
        return {
            accountThingCount: 0,
            deviceThingCount: 0,
            totalThingCount: 0
        };
    }
}

/**
 * 将今天的统计数据保存到S3
 * @param {Object} data - 要保存的数据
 */
async function saveTodayStatisticsToS3(data) {
    try {
        // 计算今天的日期和S3键名
        const today = new Date().toISOString().split('T')[0];
        const year = today.split('-')[0];
        const month = today.split('-')[1];
        const key = `${year}/${month}/daily-statistic-${today}.json`;

        const command = new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: `${S3_KEY_PREFIX}${key}`,
            Body: JSON.stringify(data, null, 2),
            ContentType: "application/json"
        });

        await s3Client.send(command);
        console.log(`今日统计数据已保存到S3: ${key}`);
    } catch (error) {
        console.error(`保存今日统计数据到S3失败: ${error.message}`);
        throw error;
    }
}

/**
 * 等待Redshift查询完成
 * @param {string} queryId - 查询ID
 */
async function waitForQueryCompletion(queryId) {
    let status = 'SUBMITTED';
    let attempts = 0;
    const maxAttempts = 60; // 最多等待60秒

    while ((status === 'SUBMITTED' || status === 'PICKED' || status === 'STARTED') && attempts < maxAttempts) {
        const statusCommand = new DescribeStatementCommand({
            Id: queryId
        });

        const statusResponse = await redshiftDataClient.send(statusCommand);
        status = statusResponse.Status;

        if (status === 'FAILED') {
            throw new Error(`Redshift查询失败: ${statusResponse.Error}`);
        }

        if (status === 'FINISHED') {
            break;
        }

        // 等待1秒后再次检查
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
    }

    if (attempts >= maxAttempts) {
        throw new Error('Redshift查询超时');
    }
}

/**
 * 从DynamoDB计数器获取设备总数
 * @param {string} counterName - 计数器名称
 * @returns {Promise<number>} - 设备总数
 */
// async function getDeviceCountFromDynamoDB(counterName) {
//     try {
//         const command = new GetItemCommand({
//             TableName: DYNAMO_COUNTER_TABLE,
//             Key: {
//                 "CounterName": { S: counterName }
//             }
//         });
//
//         const response = await dynamoClient.send(command);
//         const count = response.Item ? parseInt(response.Item.Count.N) : 0;
//         console.log(`从DynamoDB获取${counterName}计数: ${count}`);
//         return count;
//     } catch (error) {
//         console.error(`从DynamoDB获取${counterName}计数失败:`, error);
//         return 0;
//     }
// }

/**
 * 从Redshift查询account表统计
 * @returns {Promise<number>} - account表记录数
 */
async function getAccountCountFromRedshift() {
    try {
        const sql = `SELECT COUNT(*) FROM device.account`;

        const executeCommand = new ExecuteStatementCommand({
            ClusterIdentifier: REDSHIFT_CLUSTER,
            DbUser: REDSHIFT_DATABASE_USER, // 添加这个参数
            Database: REDSHIFT_DATABASE,
            Sql: sql
        });

        console.log('执行Redshift查询: device.account表统计');
        const executeResponse = await redshiftDataClient.send(executeCommand);
        await waitForQueryCompletion(executeResponse.Id);

        const resultsCommand = new GetStatementResultCommand({
            Id: executeResponse.Id
        });

        const results = await redshiftDataClient.send(resultsCommand);

        if (results.Records && results.Records.length > 0) {
            const count = results.Records[0][0]?.longValue || 0;
            console.log(`从Redshift获取account表记录数: ${count}`);
            return count;
        }

        return 0;
    } catch (error) {
        console.error('从Redshift获取account表统计失败:', error);
        return 0;
    }
}

/**
 * 从Redshift查询device表统计
 * @returns {Promise<number>} - account表记录数
 */
async function getDeviceCountFromRedshift() {
    try {
        const sql = `SELECT count(*) from device.device WHERE  account_id !=0 and topic != '' and (type in ('LT_WF','BM') or sku in ('H5151', 'H5041', 'H5043', 'H5042', 'H5044','H5106','H5140'))`;

        const executeCommand = new ExecuteStatementCommand({
            ClusterIdentifier: REDSHIFT_CLUSTER,
            DbUser: REDSHIFT_DATABASE_USER, // 添加这个参数
            Database: REDSHIFT_DATABASE,
            Sql: sql
        });

        console.log('执行Redshift查询: device.device表统计');
        const executeResponse = await redshiftDataClient.send(executeCommand);
        await waitForQueryCompletion(executeResponse.Id);

        const resultsCommand = new GetStatementResultCommand({
            Id: executeResponse.Id
        });

        const results = await redshiftDataClient.send(resultsCommand);

        if (results.Records && results.Records.length > 0) {
            const count = results.Records[0][0]?.longValue || 0;
            console.log(`从Redshift获取device表记录数: ${count}`);
            return count;
        }

        return 0;
    } catch (error) {
        console.error('从Redshift获取device表统计失败:', error);
        return 0;
    }
}

async function getGatewayDeviceCountFromRedshift() {
    try {
        const sql = `SELECT count(*) from device.gateway WHERE account_id !=0 and topic != ''`;

        const executeCommand = new ExecuteStatementCommand({
            ClusterIdentifier: REDSHIFT_CLUSTER,
            DbUser: REDSHIFT_DATABASE_USER, // 添加这个参数
            Database: REDSHIFT_DATABASE,
            Sql: sql
        });

        console.log('执行Redshift查询: device.gateway表统计');
        const executeResponse = await redshiftDataClient.send(executeCommand);
        await waitForQueryCompletion(executeResponse.Id);

        const resultsCommand = new GetStatementResultCommand({
            Id: executeResponse.Id
        });

        const results = await redshiftDataClient.send(resultsCommand);

        if (results.Records && results.Records.length > 0) {
            const count = results.Records[0][0]?.longValue || 0;
            console.log(`从Redshift获取gateway表记录数: ${count}`);
            return count;
        }

        return 0;
    } catch (error) {
        console.error('从Redshift获取gateway表统计失败:', error);
        return 0;
    }
}

/**
 * 格式化IoT统计消息
 * @param {Object} statistics - IoT统计数据
 * @returns {Object} - 格式化的钉钉消息
 */
function formatIoTStatisticsMessage(statistics) {
    const now = new Date().toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
    });

    const today = new Date().toISOString().split('T')[0];

    let markdown = `
# AWS IoT Core Thing统计日报 - ${today}
> 生成时间: ${now}

## 📊 总体概况
- Thing总数: **${statistics.totalThingCount.toLocaleString()}**
- 今日净增量: **${statistics.totalThingIncrement >= 0 ? '+' : ''}${statistics.totalThingIncrement.toLocaleString()}**

## 📈 设备类型分布
- Account类型: **${statistics.accountThingCount.toLocaleString()}** (增量: ${statistics.accountThingIncrement >= 0 ? '+' : ''}${statistics.accountThingIncrement.toLocaleString()})
- Device类型: **${statistics.deviceThingCount.toLocaleString()}** (增量: ${statistics.deviceThingIncrement >= 0 ? '+' : ''}${statistics.deviceThingIncrement.toLocaleString()})

## 📊 类型占比
- Account类型占比: **${statistics.totalThingCount > 0 ? ((statistics.accountThingCount / statistics.totalThingCount) * 100).toFixed(2) : 0}%**
- Device类型占比: **${statistics.totalThingCount > 0 ? ((statistics.deviceThingCount / statistics.totalThingCount) * 100).toFixed(2) : 0}%**

---
*数据来源: Redshift (device.account, device.device), S3 (历史对比)*`;

    const message = {
        msgtype: "markdown",
        markdown: {
            title: `AWS IoT Core 设备统计日报 - ${today}`,
            text: markdown,
        },
    };

    return message;
}

/**
 * 获取AWS IoT Core统计数据的主函数
 * @returns {Promise<Object>} - 格式化的钉钉消息和统计数据
 */
async function getAWSIotCoreStatistic() {
    try {
        console.log('开始获取AWS IoT Core统计数据...');

        // 并行获取今日数据
        const [accountThingCount, sdeviceThingCount, gatewayThingCount, yesterday] = await Promise.all([
            getAccountCountFromRedshift(),
            getDeviceCountFromRedshift(), // 从DynamoDB获取device计数
            getGatewayDeviceCountFromRedshift(), // 从DynamoDB获取device计数
            getYesterdayStatisticsFromS3()
        ]);
        let deviceThingCount = sdeviceThingCount + gatewayThingCount;
        const totalThingCount = accountThingCount + deviceThingCount;

        // 计算增量
        const yesterdayAccountThingCount = yesterday.accountThingCount || 0;
        const yesterdayDeviceThingCount = yesterday.deviceThingCount || 0;
        const yesterdayTotalThingCount = yesterday.totalThingCount || 0;

        const accountThingIncrement = accountThingCount - yesterdayAccountThingCount;
        const deviceThingIncrement = deviceThingCount - yesterdayDeviceThingCount;
        const totalThingIncrement = totalThingCount - yesterdayTotalThingCount;

        // 构建统计消息对象
        const statisticMessage = {
            date: new Date().toISOString().split('T')[0],
            timestamp: new Date().toISOString(),
            accountThingCount: accountThingCount,
            deviceThingCount: deviceThingCount,
            totalThingCount: totalThingCount,
            accountThingIncrement: accountThingIncrement,
            deviceThingIncrement: deviceThingIncrement,
            totalThingIncrement: totalThingIncrement,
            yesterdayData: {
                accountThingCount: yesterdayAccountThingCount,
                deviceThingCount: yesterdayDeviceThingCount,
                totalThingCount: yesterdayTotalThingCount
            }
        };

        console.log('统计数据:', statisticMessage);


        // 保存今日统计数据到S3
        await saveTodayStatisticsToS3(statisticMessage);

        console.log('AWS IoT Core统计数据获取完成');

        return statisticMessage;

    } catch (error) {
        console.error('获取AWS IoT Core统计数据失败:', error);

        // 返回错误消息
        const errorMessage = {
            msgtype: "text",
            text: {
                content: `AWS IoT Core统计数据获取失败: ${error.message}`
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
    getAWSIotCoreStatistic,
    formatIoTStatisticsMessage,
};
