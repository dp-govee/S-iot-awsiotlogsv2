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
        return {
            msgtype: 'markdown',
            markdown: {
                title: '⚠️ AWS IoT Core 告警',
                text: markdownMessage
            }
        }
    } catch (error) {
        console.error('解析告警消息失败:', error);
        // 如果解析失败，返回原始消息
        return `## AWS IoT Core 告警通知\n\n**原始消息**: ${snsMessage.Message}`;
    }
}

export {parseAlarmMessage};