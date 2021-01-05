-- # 在你的 mysql 创建这个表
CREATE TABLE `danjuan_fund` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `fund_name` varchar(64) DEFAULT ' COMMENT '基金名称',
    `fund_code` varchar(16) NOT NULL DEFAULT ' COMMENT '基金代码',
    `managers` varchar(32) NOT NULL DEFAULT ' COMMENT '管理人',
    `enddate` varchar(32) NOT NULL DEFAULT ' COMMENT '季报日期',
    `type` varchar(32) NOT NULL DEFAULT ' COMMENT '基金类型',
    `detail_json` text NOT NULL COMMENT '蛋卷基金详细信息json',
    PRIMARY KEY (`id`),
    KEY `idx_code` (`fund_code`),
    KEY `idx_name` (`fund_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;