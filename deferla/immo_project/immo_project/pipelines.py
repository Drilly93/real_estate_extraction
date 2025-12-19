# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from scrapy.pipelines.images import ImagesPipeline
from scrapy import Request

class ImmoProjectPipeline:
    def process_item(self, item, spider):
        return item


class CustomImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        for i, image_url in enumerate(item.get('image_urls', [])):
            yield Request(image_url, meta={'bien_id': item.get('id'), 'index': i + 1})

    def file_path(self, request, response=None, info=None, *, item=None):
        bien_id = request.meta['bien_id']
        index = request.meta['index']
        
        return f'{bien_id}/image_{index}.jpg'
    
    