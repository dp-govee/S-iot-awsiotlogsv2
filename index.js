import axios from 'axios';
import crypto from 'crypto';
// 钉钉机器人webhook地址
const DINGTALK_WEBHOOK = 'https://oapi.dingtalk.com/robot/send?access_token=1729abb4f43b16e5810bdbf717fd54fee0cebaeb4f64ac20d18efd8b72e754eb';
const Secret = 'SECaa194635884805ef26a2fcd379875245d1d31012fd44e4daa97a36f6d7c241b5';

/**
 * 发送消息到钉钉
 * @param {Object} message - 要发送的消息内容
 * @returns {Promise} - 请求的Promise
 */
async function sendToDingTalk(message) {
    try {
        // 生成签名
        let sign = '';
        const timestamp = Date.now(); // 当前时间戳（毫秒）
        const hmac = crypto.createHmac('sha256', Secret);
        hmac.update(Buffer.from(`${timestamp}\n${Secret}`, 'utf8'));
        sign = encodeURIComponent(hmac.digest('base64'));
        const webhookUrl = `${DINGTALK_WEBHOOK}&timestamp=${timestamp}&sign=${sign}`;
        const response = await axios.post(webhookUrl, {
            msgtype: 'markdown',
            markdown: {
                title: '⚠️ AWS IoT Core 告警',
                text: message
            }
        });
        console.log('钉钉响应:', response.data);
        return response.data;
    } catch (error) {
        console.error('发送到钉钉失败:', error);
        throw error;
    }
}

/**
 * 处理SNS订阅确认 confirm 订阅
 * @param {Object} message - SNS消息对象
 * @returns {Promise} - 确认请求的Promise
 */
async function confirmSubscription(message) {
    const subscribeUrl = message.SubscribeURL;
    if (!subscribeUrl) {
        const error = new Error('SNS确认消息中没有SubscribeURL');
        console.error(error);
        throw error;
    }
    console.log('正在确认SNS订阅:', subscribeUrl);
    try {
        // 发送GET请求确认订阅
        const response = await axios.get(subscribeUrl);
        console.log('SNS订阅确认响应:', response.data);

        return response.data;
    } catch (error) {
        console.error('SNS订阅确认失败:', error);
        throw error;
    }
}

/**
 * 解析SNS消息中的CloudWatch告警信息
 * @param {Object} snsMessage - SNS消息对象
 * @returns {String} - 格式化的告警消息
 */
function parseAlarmMessage(snsMessage) {
    try {
        // 尝试解析告警消息
        const alarmMessage = JSON.parse(snsMessage.Message);

        // 提取关键信息
        const alarmName = alarmMessage.AlarmName || '未知告警';
        const alarmDescription = alarmMessage.AlarmDescription || '无描述';
        const newStateValue = alarmMessage.NewStateValue || '未知状态';
        const newStateReason = alarmMessage.NewStateReason || '无原因';
        const timestamp = alarmMessage.StateChangeTime || new Date().toISOString();

        // 构建Markdown格式的消息
        let markdownMessage = `## ⚠️ AWS IoT Core 告警通知\n\n`;
        markdownMessage += `**告警名称**: ${alarmName}\n\n`;
        markdownMessage += `**告警状态**: ${newStateValue}\n\n`;
        markdownMessage += `**告警描述**: ${alarmDescription}\n\n`;
        markdownMessage += `**告警原因**: ${newStateReason}\n\n`;
        markdownMessage += `**触发时间**: ${timestamp}\n\n`;

        // 添加告警详情链接
        if (alarmMessage.AWSAccountId && alarmMessage.Region) {
            const region = 'us-east-1';
            const accountId = alarmMessage.AWSAccountId;
            markdownMessage += `[查看告警详情](https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#alarmsV2:alarm/${alarmName}?~(accountId~'${accountId}))\n\n`;
        }
        return markdownMessage;
    } catch (error) {
        console.error('解析告警消息失败:', error);
        // 如果解析失败，返回原始消息
        return `## AWS IoT Core 告警通知\n\n**原始消息**: ${snsMessage.Message}`;
    }
}

export const handler = async (event) => {
    console.log('收到事件:', JSON.stringify(event, null, 2));

    try {
        // 检查是否是SNS消息
        if (event.Records && event.Records.length > 0 && event.Records[0].EventSource === 'aws:sns') {
            const snsMessage = event.Records[0].Sns;
            console.log('SNS消息:', JSON.stringify(snsMessage, null, 2));

            // 检查是否是订阅确认消息
            if (snsMessage.Type === 'SubscriptionConfirmation') {
                console.log('收到SNS订阅确认请求');
                await confirmSubscription(snsMessage);
                console.log('SNS订阅确认成功');
                return {
                    statusCode: 200,
                    body: JSON.stringify({ message: 'SNS订阅确认成功' }),
                };
            }
            // 处理正常的通知消息
            else if (snsMessage.Type === 'Notification') {
                console.log('收到SNS通知消息');
                // 解析告警消息
                const formattedMessage = parseAlarmMessage(snsMessage);
                // 发送到钉钉
                await sendToDingTalk(formattedMessage);
                console.log('成功发送告警消息到钉钉');
            }
            else {
                console.log(`未知的SNS消息类型: ${snsMessage.Type}`);
            }
        } else {
            console.log('非SNS消息，跳过处理');
        }
        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Lambda 执行成功！' }),
        };
    } catch (error) {
        console.error('处理消息失败:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: '处理消息失败', error: error.message }),
        };
    }
};