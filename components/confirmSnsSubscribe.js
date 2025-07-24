
import axios from 'axios';

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
export {
  confirmSubscription
}