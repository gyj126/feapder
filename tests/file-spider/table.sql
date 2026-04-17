-- FileSpider 任务表
CREATE TABLE IF NOT EXISTS `file_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `file_urls` text COMMENT '待下载文件URL列表，JSON数组格式',
  `state` int(11) DEFAULT 0 COMMENT '任务状态: 0待做 2下载中 1完成 -1失败',
  PRIMARY KEY (`id`),
  KEY `idx_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 结果表（场景三使用）
CREATE TABLE IF NOT EXISTS `file_result` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` int(11) DEFAULT NULL COMMENT '任务ID',
  `result_urls` text COMMENT '文件存储位置列表，JSON数组，与file_urls位置对应',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例数据
INSERT INTO `file_task` (`file_urls`, `state`) VALUES
('["https://httpbin.org/image/png", "https://httpbin.org/image/jpeg"]', 0),
('["https://httpbin.org/image/svg", "https://httpbin.org/image/webp", "https://httpbin.org/image/png"]', 0);
