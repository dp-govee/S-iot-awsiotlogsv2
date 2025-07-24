import { IoTClient, ListThingsCommand, ListThingTypesCommand } from "@aws-sdk/client-iot";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { Readable } from 'stream';

// 初始化客户端
const iotClient = new IoTClient({ region: "us-east-1" });
const s3Client = new S3Client({ region: "us-east-1" });

// S3 存储配置
//iotcore-logs/2025/07/24/02/pro-iotcore-logs-4-2025-07-24-02-15-29-978e2041-953e-4335-a931-e515adac9fe9.gz
const S3_BUCKET = "govee-logs";
const S3_KEY_PREFIX = "iotcore-logs/";

/**
 * 从S3获取存储的统计数据
 * @param {string} key - S3对象键名
 * @returns {Promise<Object>} - 存储的统计数据
 */
async function getStatisticsFromS3(key) {
    try {
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
            // 处理非流式响应
            const data = await response.Body.transformToString();
            return JSON.parse(data);
        }
    } catch (error) {
        // 如果对象不存在或其他错误，返回null
        console.log(`从S3获取统计数据失败: ${error.message}`);
        return null;
    }
}

/**
 * 将统计数据保存到S3
 * @param {string} key - S3对象键名
 * @param {Object} data - 要保存的数据
 */
async function saveStatisticsToS3(key, data) {
    try {
        const command = new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: `${S3_KEY_PREFIX}${key}`,
            Body: JSON.stringify(data),
            ContentType: "application/json"
        });
        
        await s3Client.send(command);
        console.log(`统计数据已保存到S3: ${key}`);
    } catch (error) {
        console.error(`保存统计数据到S3失败: ${error.message}`);
        throw error;
    }
}

/**
 * 获取所有IoT things的总数
 * @returns {Promise<number>} - things总数
 */
async function getThingsCount() {
    let totalThings = 0;
    let nextToken = null;
    
    try {
        do {
            const command = new ListThingsCommand({
                maxResults: 250,  // 使用分页查询，每页250个
                nextToken: nextToken
            });
            
            const response = await iotClient.send(command);
            totalThings += response.things?.length || 0;
            nextToken = response.nextToken;
            
            // 添加短暂延迟以避免API限流
            if (nextToken) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        } while (nextToken);
        
        return totalThings;
    } catch (error) {
        console.error(`获取IoT things总数失败: ${error.message}`);
        throw error;
    }
}

/**
 * 获取特定thingType的things数量
 * @param {string} thingType - 要查询的thingType名称
 * @returns {Promise<number>} - 符合条件的things数量
 */
async function getThingsByType(thingType) {
    let matchingThings = 0;
    let nextToken = null;
    
    try {
        do {
            const command = new ListThingsCommand({
                maxResults: 250,
                thingTypeName: thingType,
                nextToken: nextToken
            });
            
            const response = await iotClient.send(command);
            matchingThings += response.things?.length || 0;
            nextToken = response.nextToken;
            
            // 添加短暂延迟以避免API限流
            if (nextToken) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        } while (nextToken);
        
        return matchingThings;
    } catch (error) {
        console.error(`获取thingType为${thingType}的things数量失败: ${error.message}`);
        throw error;
    }
}

/**
 * 获取IoT统计数据
 * @returns {Promise<Object>} - IoT统计数据
 */
async function getIoTStatistics() {
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    const year = today.split('-')[0]
    const month =today.split('-')[1]
    // 尝试从S3获取昨天的统计数据
    const yesterdayKey = `${year}/${month}/daily-statistic-${yesterday}.json`;
    const yesterdayStats = await getStatisticsFromS3(yesterdayKey);
    
    // 获取当前的things总数
    const totalThings = await getThingsCount();
    
    // 计算增量
    const increment = yesterdayStats ? totalThings - yesterdayStats.totalThings : 0;
    
    // 获取特定thingType的things数量
    const accountTypeThings = await getThingsByType('account');
    
    // 构建今天的统计数据
    const todayStats = {
        date: today,
        totalThings: totalThings,
        increment: increment,
        accountTypeThings: accountTypeThings,
        deviceTypeThings: totalThings - accountTypeThings,
        timestamp: new Date().toISOString()
    };
    
    // 保存今天的统计数据到S3
    const todayKey = `${year}/${month}/daily-statistic-${today}.json`;
    await saveStatisticsToS3(todayKey, todayStats);
    
    return todayStats;
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

    let markdown = `
# AWS IoT Core 设备统计日报 - ${statistics.date}
> 生成时间: ${now}

## 设备统计
- 设备总数: **${statistics.totalThings}**
- 今日新增: **${statistics.increment}**
- Account类型设备: **${statistics.accountTypeThings}**

`;

    const message = {
        msgtype: "markdown",
        markdown: {
            title: `AWS IoT Core 设备统计日报 - ${statistics.date}`,
            text: markdown,
        },
    };

    return message;
}

export { getIoTStatistics, formatIoTStatisticsMessage };
