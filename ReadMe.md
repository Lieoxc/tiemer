## 目的

1. 目前golang中常用的github.com/ouqiang/gocron、或者robfig/cron 都是单点，不支持集群部署，多实例会导致重复运行。
2. 设备定时策略，延时命令下发、运维数据分析等大量规则灵活多变