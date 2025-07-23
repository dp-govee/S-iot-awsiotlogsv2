import Koa from 'koa';
import bodyParser from 'koa-bodyparser';
import cors from '@koa/cors'; // 确保安装了 @koa/cors 包
import axios from 'axios';
import { handler } from './index.js'; // 引用适配 Lambda 入口文件

const app = new Koa();

app.use(cors());
app.use(bodyParser({enableTypes: ['json', 'form', 'text']}));

app.use(async (ctx) => {
    // 添加 /ping 接口
    if (ctx.path === '/ping' && ctx.method === 'GET') {
        ctx.status = 200;
        ctx.body = {message: 'pong'};
        return;
    }
    try {
        //iot 路由消息确认
        let headers = ctx.request.headers;
        let destinationConfirmation = headers['x-amz-rules-engine-message-type'];
        //确认消息需要，返回
        if ('DestinationConfirmation' === destinationConfirmation) {
            let enableUrl = ctx.request.body.enableUrl;
            await axios.get(enableUrl);
            console.log('DestinationConfirmation successfully');
            ctx.status = 200;
            ctx.body = 'DestinationConfirmation success';
            return;
        }
    } catch (e) {
        console.error('DestinationConfirmation Error', e); // 添加日志以便调试
        ctx.status = 500;
        ctx.body = {error: 'Internal Server Error'};
        return;
    }

    try {
        const response = await handler(
            ctx.request.body
        );
        ctx.status = response.statusCode || 200;
        ctx.response.type = 'application/json';
        ctx.body = response.body;

    } catch (error) {
        console.error('Error handling request:', error); // 添加日志以便调试
        ctx.status = 500;
        ctx.body = {error: 'Internal Server Error'};
    }
});

app.listen(8080, () => {
    console.log('Server is starting at port 8080');
    console.log(JSON.stringify(process.env));
});
