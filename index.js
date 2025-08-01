import {formatMessage, gatherStatistics} from "./components/ScheduleStatistics.js";
import {sendToDingTalk} from "./components/sendDingTalk.js";
import {parseAlarmMessage} from "./components/parseAlarmMessage.js";
import {confirmSubscription} from "./components/confirmSnsSubscribe.js";
import {getAWSIotCoreStatistic, formatIoTStatisticsMessage} from "./components/AWSIotCoreStatistic.js";
import {formatIotMsgStatisticsMessage, getAWSIotMsgStatistic} from "./components/AWSIotMsgStatistic.js";
import {getAWSIoTErrorStatistic, formatIoTErrorStatisticsMessage} from "./components/AWSIotErrorStatistic.js";

export const handler = async (event) => {
    console.log('收到事件:', JSON.stringify(event, null, 2));

    try {
        // 检查是否是SNS消息
        if (isEventFromSNS(event)) {
            const snsMessage = event.Records[0].Sns;
            console.log('SNS消息:', JSON.stringify(snsMessage, null, 2));

            // 检查是否是订阅确认消息
            if (snsMessage.Type === 'SubscriptionConfirmation') {
                console.log('收到SNS订阅确认请求');
                await confirmSubscription(snsMessage);
                console.log('SNS订阅确认成功');
                return {
                    statusCode: 200,
                    body: JSON.stringify({message: 'SNS订阅确认成功'}),
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
            } else {
                console.log(`未知的SNS消息类型: ${snsMessage.Type}`);
            }
        } else if (isEventFromEventBridgeScheduler(event)) {
            console.log('收到EventBridge定时事件:', JSON.stringify(event));

            // 检查是否是IoT统计任务

            console.log('执行IoT设备统计任务');
            // 1. 获取IoT统计数据
            const iotStatistics = await getAWSIotCoreStatistic()

            // 2. 格式化IoT统计消息
            const dingTalkMessage = formatIoTStatisticsMessage(iotStatistics);

            // 3. 发送到钉钉
            await sendToDingTalk(dingTalkMessage);

            console.log('执行IoT消息统计任务');
            //2.获取Iot消息统计数据
            const iotMsgStatistics = await getAWSIotMsgStatistic();
            // 格式化钉钉消息
            const dingTalkIotMsg =formatIotMsgStatisticsMessage(iotMsgStatistics);
            // 3. 发送到钉钉
            await sendToDingTalk(dingTalkIotMsg);


            console.log('执行IoT错误消息统计任务');
            //2.获取Iot消息统计数据
            const iotErrorMsgStatistics = await getAWSIoTErrorStatistic();
            // 格式化钉钉消息
            const dingTalkIotErrorMsg =formatIoTErrorStatisticsMessage(iotErrorMsgStatistics);
            // 3. 发送到钉钉
            await sendToDingTalk(dingTalkIotErrorMsg);

            console.log('成功发送IoT统计消息到钉钉');
        }
        return {
            statusCode: 200,
            body: JSON.stringify({message: 'Lambda 执行成功！'}),
        };
    } catch (error) {
        console.error('处理消息失败:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({message: '处理消息失败', error: error.message}),
        };
    }
};

/**
 * 判断事件是否来自SNS
 * @param {Object} event - 事件对象
 * @returns {boolean} - 是否来自SNS
 */
function isEventFromSNS(event) {
    return event.Records &&
        event.Records.length > 0 &&
        event.Records[0].EventSource === 'aws:sns';
}

/**
 * 判断事件是否来自 EventBridge 定时触发
 * @param {Object} event - 事件对象
 * @returns {boolean} - 是否来自EventBridge定时触发
 */
function isEventFromEventBridgeScheduler(event) {
    // EventBridge 定时事件通常具有以下结构
    return (
        event.source === 'aws.events' &&
        (event['detail-type'] === 'Scheduled Event' ||
            event['detail-type'] === 'EventBridge Scheduled Event')
    ) || (
        // 兼容旧版 CloudWatch Events 格式
        event.source === 'aws.events' &&
        event.resources &&
        event.resources[0] &&
        event.resources[0].includes('rule/')
    );
}

console.log(isEventFromEventBridgeScheduler({
    "version": "0",
    "id": "d31118fc-c040-c429-ba6a-597be83dab98",
    "detail-type": "Scheduled Event",
    "source": "aws.events",
    "account": "920700710667",
    "time": "2025-07-28T02:00:00Z",
    "region": "us-east-1",
    "resources": [
        "arn:aws:events:us-east-1:920700710667:rule/pro-iotcore-logs"
    ],
    "detail": {}
}))