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
        const response = await axios.post(webhookUrl, message);
        console.log('钉钉响应:', response.data);
        return response.data;
    } catch (error) {
        console.error('发送到钉钉失败:', error);
        throw error;
    }
}

export {
    sendToDingTalk
}