#!/bin/bash

# 一键打包部署脚本 - AWS IoT Core 告警通知 Lambda 函数
# 作者: Amazon Q
# 日期: 2025-07-24

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 函数名称
FUNCTION_NAME="AWSIoTCoreAlarm"
REGION="us-east-1"
DEPLOY_PACKAGE="function-deploy.zip"

echo -e "${YELLOW}开始部署 AWS IoT Core 告警通知 Lambda 函数...${NC}"

# 清理旧的部署包
echo -e "${YELLOW}清理旧的部署包...${NC}"
if [ -f "$DEPLOY_PACKAGE" ]; then
    rm "$DEPLOY_PACKAGE"
fi

# 创建部署包
echo -e "${YELLOW}创建部署包...${NC}"
zip -r "$DEPLOY_PACKAGE" index.js components/ package.json node_modules/ > /dev/null

# 检查部署包是否创建成功
if [ ! -f "$DEPLOY_PACKAGE" ]; then
    echo -e "${RED}错误: 部署包创建失败。${NC}"
    exit 1
fi

# 获取部署包大小
PACKAGE_SIZE=$(du -h "$DEPLOY_PACKAGE" | cut -f1)
echo -e "${GREEN}部署包创建成功! 大小: $PACKAGE_SIZE${NC}"

# 部署 Lambda 函数
echo -e "${YELLOW}正在更新 Lambda 函数代码...${NC}"
aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$DEPLOY_PACKAGE" \
    --region "$REGION" > /dev/null

# 检查部署是否成功
if [ $? -ne 0 ]; then
    echo -e "${RED}错误: Lambda 函数更新失败。${NC}"
    exit 1
fi

# 等待函数更新完成
echo -e "${YELLOW}等待函数更新完成...${NC}"
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"

# 获取函数信息
FUNCTION_INFO=$(aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" --query "Configuration.[LastModified,CodeSize,Runtime,MemorySize,Timeout]" --output text)
LAST_MODIFIED=$(echo "$FUNCTION_INFO" | awk '{print $1}')
CODE_SIZE=$(echo "$FUNCTION_INFO" | awk '{print $2}')
CODE_SIZE_MB=$(echo "scale=2; $CODE_SIZE/1048576" | bc)
RUNTIME=$(echo "$FUNCTION_INFO" | awk '{print $3}')
MEMORY=$(echo "$FUNCTION_INFO" | awk '{print $4}')
TIMEOUT=$(echo "$FUNCTION_INFO" | awk '{print $5}')

echo -e "${GREEN}部署成功!${NC}"
echo -e "${GREEN}函数名称: ${NC}$FUNCTION_NAME"
echo -e "${GREEN}运行时: ${NC}$RUNTIME"
echo -e "${GREEN}内存: ${NC}$MEMORY MB"
echo -e "${GREEN}超时: ${NC}$TIMEOUT 秒"
echo -e "${GREEN}代码大小: ${NC}$CODE_SIZE_MB MB"
echo -e "${GREEN}最后更新: ${NC}$LAST_MODIFIED"

exit 0
