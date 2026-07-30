-- FileSpider 任务表
-- file_urls 供直链模式使用（场景一、四、五）
-- file_ids  供接口模式使用（场景二、三），直链需先请求下载接口换取
CREATE TABLE IF NOT EXISTS `file_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `file_urls` text COMMENT '待下载文件URL列表，JSON数组格式',
  `file_ids` text COMMENT '待下载文件ID列表，JSON数组格式',
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

-- 示例数据（直链模式）
INSERT INTO `file_task` (`file_urls`, `state`) VALUES
('["https://httpbin.org/image/png", "https://httpbin.org/image/jpeg"]', 0),
('["https://httpbin.org/image/svg", "https://httpbin.org/image/webp", "https://httpbin.org/image/png"]', 0);

-- 示例数据（接口模式）
INSERT INTO `file_task` (`file_ids`, `state`) VALUES
('["f1001", "f1002", "f1003"]', 0);
