# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import pymysql

class TextPipeline:
    def __init__(self):
        self.limit = 50

    def process_item(self, item, spider):
        if item['text']:
            if len(item['text']) > self.limit:
                item['text'] = item['text'][:self.limit].rstrip() + '...'
            return item
        else:
            raise DropItem('Missing Text')

class MysqlPipeline:
    def __init__(self, connection_string, database):
        self.connection_string = connection_string
        self.database = database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            connection_string=crawler.settings.get('CONNECTION_STRING'),
            database=crawler.settings.get('DATABASE')
        )

    def open_spider(self, spider):
        # 第一步：连接到 MySQL 服务器（不指定数据库）
        temp_conn = pymysql.connect(
            host=self.connection_string['host'],
            user=self.connection_string['user'],
            password=self.connection_string['password'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            with temp_conn.cursor() as cursor:
                # 创建数据库（如果不存在）
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            temp_conn.commit()
        finally:
            temp_conn.close()

        # 第二步：连接到目标数据库
        self.client = pymysql.connect(
            host=self.connection_string['host'],
            user=self.connection_string['user'],
            password=self.connection_string['password'],
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.client.cursor()

    def process_item(self, item, spider):
        create_table_sql="""
            CREATE TABLE IF NOT EXISTS quotes(
                id int AUTO_INCREMENT PRIMARY KEY,
                text varchar(255),
                author varchar(255),
                tags varchar(255)
            )charset=utf8mb4,COLLATE utf8mb4_unicode_ci;
        """
        insert_sql="INSERT INTO quotes(text,author,tags) VALUES (%s,%s,%s);"
        self.cursor.execute(create_table_sql)
        self.cursor.execute(insert_sql,(item['text'],item['author'],"".join(item['tags'])))
        self.client.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.client.close()