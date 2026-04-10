# -*- coding: utf-8 -*-
"""
Created on 2026-04-10
---------
@summary: 创建KafkaItem
---------
"""

import getpass
import os

import feapder.utils.tools as tools
from .create_init import CreateInit


def deal_file_info(file):
    file = file.replace("{DATE}", tools.get_current_date())
    file = file.replace("{USER}", os.getenv("FEAPDER_USER") or getpass.getuser())

    return file


class CreateKafkaItem:
    def __init__(self):
        self._create_init = CreateInit()

    def convert_topic_name_to_hump(self, topic_name):
        table_hump_format = ""

        words = topic_name.split("_")
        for word in words:
            table_hump_format += word.capitalize()

        return table_hump_format

    def get_template(self):
        template_path = os.path.abspath(
            os.path.join(__file__, "../../../templates/kafka_item_template.tmpl")
        )
        with open(template_path, "r", encoding="utf-8") as file:
            template = file.read()

        return template

    def save_template_to_file(self, item_template, topic_name):
        item_file = topic_name + "_item.py"
        if os.path.exists(item_file):
            confirm = input("%s 文件已存在 是否覆盖 (y/n).  " % item_file)
            if confirm != "y":
                print("取消覆盖  退出")
                return

        with open(item_file, "w", encoding="utf-8") as file:
            file.write(item_template)
            print("\n%s 生成成功" % item_file)

        if os.path.basename(os.path.dirname(os.path.abspath(item_file))) == "items":
            self._create_init.create()

    def create(self, topic_name):
        template = self.get_template()

        item_name_hump = self.convert_topic_name_to_hump(topic_name)
        template = template.replace("${item_name}", item_name_hump)
        template = template.replace("${command}", topic_name)
        template = template.replace("${table_name}", topic_name)
        template = deal_file_info(template)

        self.save_template_to_file(template, topic_name)
