@echo off

REM 检查参数是否为空
IF "%1"=="" (
    echo 请传入目录参数
    exit /b
)
set GOOS=linux

REM 进入对应目录
cd cmd/%1

REM 执行go build
go build

cd ../..