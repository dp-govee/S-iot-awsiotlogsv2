import {
    AthenaClient,
    StartQueryExecutionCommand,
    GetQueryExecutionCommand,
    GetQueryResultsCommand
} from "@aws-sdk/client-athena";

// 初始化 Athena 客户端
const client = new AthenaClient({ region: "us-east-1" });

/**
 * 收集 IoT 设备错误统计数据
 */
async function gatherStatistics() {
    const query = `
    SELECT 
        clientId, 
        COUNT(*) as error_count
    FROM 
        your_database.iot_logs
    WHERE 
        logLevel = 'ERROR' AND
        date = CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY 
        clientId
    ORDER BY 
        error_count DESC
    LIMIT 10
  `;

    // 启动查询
    const startCommand = new StartQueryExecutionCommand({
        QueryString: query,
        ResultConfiguration: {
            OutputLocation: "s3://your-bucket/query-results/",
        },
    });

    const startResponse = await client.send(startCommand);
    const queryExecutionId = startResponse.QueryExecutionId;

    // 等待查询完成
    let queryStatus;
    do {
        const statusCommand = new GetQueryExecutionCommand({
            QueryExecutionId: queryExecutionId,
        });

        queryStatus = await client.send(statusCommand);
        const state = queryStatus.QueryExecution.Status.State;

        if (state === "FAILED") {
            throw new Error(
                `Athena 查询失败: ${queryStatus.QueryExecution.Status.StateChangeReason}`
            );
        }

        if (state === "SUCCEEDED") {
            break;
        }

        // 等待 1 秒再检查
        await new Promise((resolve) => setTimeout(resolve, 1000));
    } while (true);

    // 获取查询结果
    const resultsCommand = new GetQueryResultsCommand({
        QueryExecutionId: queryExecutionId,
    });

    const results = await client.send(resultsCommand);

    // 处理结果
    const statistics = {
        date: new Date().toISOString().split("T")[0],
        total_errors: 0,
        top_clients: [],
    };

    // 跳过标题行
    const rows = results.ResultSet.Rows.slice(1);
    for (const row of rows) {
        const data = row.Data;
        const clientId = data[0]?.VarCharValue || "unknown";
        const errorCount = parseInt(data[1]?.VarCharValue || "0");

        statistics.total_errors += errorCount;
        statistics.top_clients.push({
            client_id: clientId,
            error_count: errorCount,
        });
    }

    return statistics;
}

/**
 * 格式化钉钉消息内容
 * @param {Object} statistics 统计数据
 */
async function formatMessage(statistics) {
    const now = new Date().toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
    });

    let markdown = `
# IoT 设备错误日报 - ${statistics.date}
> 生成时间: ${now}

## 总体统计
- 总错误数: **${statistics.total_errors}**

## 错误最多的设备 (Top 10)
| 设备 ID | 错误次数 |
| ------ | ------- |
`;

    for (const client of statistics.top_clients) {
        markdown += `| ${client.client_id} | ${client.error_count} |\n`;
    }

    const message = {
        msgtype: "markdown",
        markdown: {
            title: `IoT 设备错误日报 - ${statistics.date}`,
            text: markdown,
        },
    };

    return message;
}

// 导出函数
export { gatherStatistics, formatMessage };
