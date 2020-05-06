import configparser
import codecs
import threading
import os
import time
import queue
import re
from requests_html import HTMLSession
import jieba


# ============================================= 引擎 ============================================================

class Engine(object):
    def __init__(self, configPath):
        self.configPath = configPath
        self.config = configparser.ConfigParser()
    
    def go(self):
        # 检查文件
        if (not os.path.exists(self.configPath)):
            print('配置文件不存在')
            return
        # 初始化所有模块
        self.downloader = None
        self.converter = None
        self.indexer = None
        self.searcher = None
        self.downloader_converter_link = False
        self.converter_indexer_link = False
        # 读取配置文件
        self.config.read(self.configPath, encoding='utf-8')
        # 配置下载器
        if ('downloader' in self.config.sections() and self.config.get('downloader', 'enable') == 'true'):
            targetPath = self.config.get('downloader', 'targetPath')
            sourceDir = self.config.get('downloader', 'sourceDir')
            includePath = self.config.get('downloader', 'includePath')
            maxDeepth = int(self.config.get('downloader', 'maxDeepth'))
            spiderPoolSize = int(self.config.get('downloader', 'spiderPoolSize'))
            self.downloader_converter_link = True if self.config.get('downloader', 'linkConverter') == 'true' else False
            self.downloader = Downloader(targetPath=targetPath, sourceDir=sourceDir, includePath=includePath, maxDeepth=maxDeepth, spiderPoolSize=spiderPoolSize)
        # 配置转换器
        if ('converter' in self.config.sections() and self.config.get('converter', 'enable') == 'true'):
            interactive = True if self.config.get('converter', 'interractive') == 'true' else False
            sourceDir = self.config.get('converter', 'sourceDir')
            doneDir = self.config.get('converter', 'doneDir')
            stopwordsPath = self.config.get('converter', 'stopwordsPath')
            resovlerPoolSize = int(self.config.get('converter', 'resolverPoolSize'))
            self.converter_indexer_link = True if self.config.get('converter', 'linkIndexer') == 'true' else False
            self.converter = Converter(interactive=interactive, doneDir=doneDir, sourceDir=sourceDir, stopwordsPath=stopwordsPath, maxResolverPool=resovlerPoolSize)
            # 如果下载器需要连接转换器则调用连接
            if (self.downloader_converter_link):
                self.downloader.linkConverter(self.converter)
        # 配置索引器
        if ('indexer' in self.config.sections() and self.config.get('indexer', 'enable') == 'true'):
            interactive = True if self.config.get('indexer', 'interractive') == 'true' else False
            self.indexer = Indexer(interactive=interactive)
            # 如果转换器需要连接索引器则调用连接
            if (self.converter_indexer_link):
                self.converter.linkIndexer(self.indexer)
        # 配置搜索器
        if ('searcher' in self.config.sections() and self.config.get('searcher', 'enable') == 'true'):
            self.searcher = Searcher()
        # 启动各个模块
        if (self.downloader != None):
            self.downloader.start()
        if (self.converter != None):
            self.converter.start()
        if (self.indexer != None):
            self.indexer.start()
        if (self.searcher != None):
            self.searcher.start()



# ============================================== 下载器模块 ===================================================

class Downloader(threading.Thread):
    """
    下载器
    管理多个爬虫，广度优先下载多个网页
    """
    def __init__(self, targetPath, sourceDir, includePath, maxDeepth=1, spiderPoolSize=16, converter=None):
        threading.Thread.__init__(self)
        self.targetPath = targetPath        # 爬取目标文件路径
        self.targets = []                   # 爬取目标
        self.sourceDir = sourceDir          # 源文件目录
        self.includePath = includePath      # 文档信息文件路径
        self.deepth = maxDeepth             # 最大爬取深度
        self.pool = spiderPoolSize          # 最大爬虫池
        self.converter = converter          # 配合的文档解析器
        self.life = True                    # 线程存活标记
        self.spiderCount = 0                # 当前爬虫数
        self.urlQueue = queue.Queue()       # 当前目标队列
        self.spiderRecord = set()           # 已经访问过的记录
        self.includeQueue = queue.Queue()   # 文档信息队列
        self.includeFile = None             # 文档信息文件
        self.writer = None                  # 文档信息写手
        # 检查目标文件路径
        if (not os.path.exists(self.targetPath)):
            print('\033[0m[Downloader] \033[1;33m目标url文件不存在！\033[0m')
            self.stop()
        else:
            targetFile = open(self.targetPath, 'r', encoding='utf-8')
            urls = targetFile.readlines()
            for url in urls:
                self.targets.append(url)
        # 检查目标数组
        if (not self.targets):
            print('\033[0m[Downloader] \033[1;33m目标为空！\033[0m')
            self.stop()
        # 检查源文件目录，不存在就创建
        if (not os.path.exists(self.sourceDir)):
            os.mkdir(self.sourceDir)
            print('\033[0m[Downloader] \033[1;33m源文件目录不存在，创建'+self.sourceDir+'\033[0m')
        # 检查文档信息文件，不存在就创建
        if (not os.path.exists(self.includePath)):
            print('\033[0m[Downloader] \033[1;33minclude文件不存在，创建'+self.includePath+'\033[0m')
        self.includeFile = open(self.includePath, 'a+', encoding='utf-8')
        self.writer = self.IncludeWriter(self.includeFile, self.includeQueue)
        print('\033[0m[Downloader] \033[1;33m初始化完成...\033[0m')

    def run(self):
        # 启动文档信息写手
        self.writer.start()
        # 将所有目标url加入目标队列
        for url in self.targets:
            self.urlQueue.put({'url': url, 'deep': 0})
        # 根据最大爬虫池和目标队列启动爬虫
        while (self.spiderCount < self.pool and not self.urlQueue.empty()):
            self.spiderStart()
        # 开始工作
        while (self.life):
            # 如果目标队列空、所有爬虫都返回并且文档信息队列为空则结束任务
            if (self.urlQueue.empty() and self.spiderCount == 0 and self.includeQueue.empty()):
                self.stop()
                print('\033[0m[Downloader] \033[1;33m任务完成！\033[0m')
            # 如果爬虫池有空余则启动新的爬虫
            elif (self.spiderCount < self.pool):
               while (self.spiderCount < self.pool and not self.urlQueue.empty()):
                    self.spiderStart()
            else:
                pass

    def stop(self):
        # 关闭文档信息文件
        self.includeFile.close()
        # 停止写手
        self.writer.stop()
        # 如果与文档转换器连接则通知转换器准备结束
        if (self.converter != None):
            self.converter.readyStop()
        self.life = False
    
    def spiderStart(self):
        """
        启动一个爬虫
        """
        # 获取目标
        aUrl = self.urlQueue.get()
        # 如果该目标没被访问过则放出爬虫
        if (aUrl['url'] not in self.spiderRecord):
            self.spiderRecord.add(aUrl['url'])
            aSpider = Spider(aUrl['url'], aUrl['deep'], self.sourceDir, self)
            aSpider.start()
            print('\033[0m[Downloader] => \033[1;32m放出Spider  目标:\033[0m'+aUrl['url'])
            self.spiderCount += 1
        # 如果目标被访问过则丢弃目标
        else:
            print('\033[0m[Downloader] => \033[1;33m丢弃目标 '+aUrl['url']+'\033[0m')

    def spiderBack(self, code, docInfo, deep):
        """
        接受一个爬虫的返回
        """
        msg = '\033[0m[Downloader] => '
        # 抓取网页失败
        if (code == -1):
            msg += '\033[1;31m失败Spider  失败:\033[0m'+docInfo['url']
        # 抓取成功
        else:
            msg += '\033[1;36m返回Spider  抓取:\033[0m'+docInfo['url']+"  \033[1;36m文件:\033[0m%d"%docInfo['hash']+'.txt'
            # 将该文档信息加入文档信息队列，交给写手去写入文件
            self.includeQueue.put(docInfo)
            # 如果有配合文档解析器，则唤醒解析器
            if (self.converter != None):
                self.converter.convert(self.sourceDir+'/%d'%docInfo['hash']+'.txt')
            # 如果还未到达最大爬取深度，当前文档信息的url压入目标队列
            if (deep+1 < self.deepth):
                links = docInfo['links']
                for link in links:
                    self.urlQueue.put({'url': link, 'deep': deep+1})
        # 释放一个爬虫池位
        self.spiderCount -= 1
        print(msg)

    def linkConverter(self, converter):
        """
        连接配合的解析器
        """
        self.converter = converter
    
    class IncludeWriter(threading.Thread):
        """
        文档信息文件的写手，防止资源冲突
        """
        def __init__(self, includeFile, includeQueue):
            threading.Thread.__init__(self)
            self.includeFile = includeFile
            self.includeQueue = includeQueue
            self.life = True
        def run(self):
            while (self.life):
                if (not self.includeQueue.empty()):
                    aInclude = self.includeQueue.get()
                    self.includeFile.write('[%d'%aInclude['hash']+']\n')
                    self.includeFile.write('hash=%d'%aInclude['hash']+'\n')
                    self.includeFile.write('time='+aInclude['time']+'\n')
                    self.includeFile.write('url='+aInclude['url']+'\n')
                    self.includeFile.write('tile='+aInclude['title']+'\n')
                    self.includeFile.write('keywords='+aInclude['keywords']+'\n')
                    self.includeFile.write('description='+aInclude['description']+'\n')
                    self.includeFile.write('linkcount=%d'%len(aInclude['links'])+'\n')
                    index = 0
                    for link in aInclude['links']:
                        self.includeFile.write('link%d='%index+link+'\n')
                        index += 1
                    self.includeFile.write('\n')
        def stop(self):
            self.life = False


class Spider(threading.Thread):
    """
    爬虫
    负责抓取网页文档以及分析网页中包含的其他链接
    """
    def __init__(self, url, deep, sourceDir, downloader):
        threading.Thread.__init__(self)
        self.url = url
        self.deep = deep
        self.sourceDir = sourceDir
        self.downloader = downloader

    def run(self):
        # 抓取网页文档
        session = HTMLSession()
        try:
            response = session.get(url=self.url)
        except:
            self.downloader.spiderBack(-1, {}, self.deep)
            return
        # 获取当前时间
        curTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        hashCode = hash(curTime+' '+self.url)
        # 将文档写入文件
        sourceFile = open(self.sourceDir+'/%d'%hashCode+'.txt', 'w+', encoding='utf-8')
        sourceFile.writelines(response.html.html)
        sourceFile.close()
        # 构造文档信息字典
        docInfo = {'hash': hashCode, 'time': curTime, 'url': self.url}
        docTitle = response.html.find('title', first=True)
        docInfo['title'] = docTitle.text if docTitle else 'none'
        docKeywords = response.html.find('mate[name=keywords]', first=True)
        docInfo['keywords'] = docKeywords.attrs['content'] if docKeywords else 'none'
        docDescription = response.html.find('mate[name=description]', first=True)
        docInfo['description'] = docDescription.attrs['content'] if docDescription else 'none'
        docLinks = []
        links = response.html.absolute_links
        for link in links:
            docLinks.append(link)
        docInfo['links'] = docLinks
        # 将获取的文档反馈给下载器
        self.downloader.spiderBack(0, docInfo, self.deep)



# ======================================== 转换器模块 =================================================

class Converter(threading.Thread):
    """
    文档转换器
    众多文档解析器的管理者
    """
    def __init__(self, doneDir, sourceDir, stopwordsPath, interactive=True, maxResolverPool=16, indexer=None):
        threading.Thread.__init__(self)
        self.done = doneDir                     # 处理完文件目录
        self.soure = sourceDir                  # 带处理文件目录
        self.stopwordsPath = stopwordsPath      # 停用词表路径
        self.pool = maxResolverPool             # 最大解析器池
        self.isInter = interactive              # 是否交互式进行
        self.indexer = indexer                  # 配合的索引构建者
        self.life = True                        # 存活标记
        self.convertQueue = queue.Queue()       # 带处理的文件队列
        self.resolverCount = 0                  # 当前解析器数量
        # 检查源文件目录
        if (not self.isInter and not os.path.exists(self.soure)):
            print('\033[0m[Converter] \033[1;33m源文件目录不存在！\033[0m')
            self.stop()
        # 检查停用词文件路径
        if (not os.path.exists(self.stopwordsPath)):
            print('\033[0m[Converter] \033[1;33m停用词表不存在！\033[0m')
            self.stop()
        else:
            stopwordsFile = open(self.stopwordsPath, encoding='utf-8')
            self.stopwords = set(stopwordsFile.read().split('\n'))
            stopwordsFile.close()
        # 检查保存目录
        if (not os.path.exists(self.done)):
            os.mkdir(self.done)
            print('\033[0m[Converter] \033[1;33m保存目录不存在，创建'+self.done+'\033[0m')
        print('\033[0m[Converter] \033[1;33m初始化完成...\033[0m')
    
    def run(self):
        # 如果式交互式运行
        if (self.isInter):
            while(self.life):
                # 如果池位空余且队列不空则启动一个解析器
                while (self.resolverCount < self.pool and not self.convertQueue.empty()):
                    self.resolverStart()
        # 如果是非交互式运行
        else:
            # 读取队列并启动所有解析器
            self.convertAll()
            while (not self.convertQueue.empty()):
                # 只要有池位空余且队列不空就启动解析器
                while (self.resolverCount < self.pool and not self.convertQueue.empty()):
                    self.resolverStart()
            # 等待所有的解析器结束
            self.readyStop()

    def convertAll(self):
        """
        根据队列大小和解析器池大小尽可能多地启动解析器
        """
        for path,d,filelist in os.walk(self.soure):
            for filename in filelist:
                self.convertQueue.put(os.path.join(path,filename))

    def convert(self, sourcePath):
        """
        响应下载器的唤醒
        """
        self.convertQueue.put(sourcePath)
    
    def resolverStart(self):
        """
        启动一个解析器
        """
        aSource = self.convertQueue.get()
        aResolver = Resolver(aSource, self.done, self.stopwords, self)
        aResolver.start()
        self.resolverCount += 1
        print('\033[0m[Converter] => \033[1;32m启动Resolver  目标:\033[0m'+aSource)

    def resolvDone(self,code, done):
        """
        接收解析器的反馈
        """
        # 源文件不存在
        if (code == -1):
            print('\033[0m[Converter] => \033[1;31mResolver错误  源文件不存在:\033[0m'+done)
        # 解文档析完成
        else:
            print('\033[0m[Converter] => \033[1;36m结束Resolver  完成:\033[0m'+done)
        # 释放一个解析器池位
        self.resolverCount -= 1
    
    def linkIndexer(self, indexer):
        """
        连接配合索引器
        """
        self.indexer = indexer

    def stop(self):
        # 如果与索引器连接则通知索引器准备结束
        if (self.indexer != None):
            self.indexer.readyStop()
        self.life = False
    
    def readyStop(self):
        """
        准备结束
        """
        while(self.resolverCount != 0):
            pass
        print('\033[0m[Converter] => \033[1;33m任务完成！\033[0m')
        self.stop()


class Resolver(threading.Thread):
    """
    文档解析器
    用于将文档转换为适合索引的字符文档
    """
    def __init__(self, sourcePath, doenDir, stopwords, converter):
        threading.Thread.__init__(self)
        self.source = sourcePath
        self.done = doenDir
        self.stopwords = stopwords
        self.converter = converter
        self.life = True
    
    def run(self):
        # 检查源文件，不存在则抛出-1
        if (not os.path.exists(self.source)):
            self.converter.resolvDone(-1, self.source)
            return
        # 拼接出目标文档文件路径
        p,sourceName = os.path.split(self.source)
        donePath = os.path.join(self.done, sourceName)
        doneFile = open(donePath, 'w+', encoding='utf-8')
        sourceFile = open(self.source, 'r', encoding='utf-8')
        # 获取源文件
        fileContent = sourceFile.read()
        sourceFile.close()
        # 去除所有换行，tab，空格
        fileContent = re.sub(re.compile('\s+'), '/', fileContent)
        # 去除包含的<javascript>标签
        fileContent = re.sub(re.compile('<script>\S*?</script>', re.IGNORECASE), '', fileContent)
        # 去除包含的<style>标签
        fileContent = re.sub(re.compile('<style[^>]*>\S*</style>', re.IGNORECASE), '', fileContent)
        # 去除该行中包含的其余标签
        fileContent = re.sub(re.compile('</?[^>]+>', re.IGNORECASE), '', fileContent)
        # 分词
        words = jieba.cut(fileContent)
        del fileContent
        # 将包含在停用词表中的词写入文档
        for word in words:
            if (word not in self.stopwords):
                doneFile.write(word+' ')
        doneFile.close()
        del words
        # 反馈给文档转换器
        self.converter.resolvDone(0, donePath)



# =====================================================  索引器模块 ===========================================

class Indexer(threading.Thread):
    """
    索引器
    用于更新倒排表
    """
    def __init__(self, interactive=True):
        pass

    def run(self):
        pass



# ====================================================== 搜索器模块 ===========================================

class Searcher(threading.Thread):
    """
    搜索器
    通过倒排表搜索文档
    """
    def __init__(self):
        pass

    def run(self):
        pass



# ======================================================== main ===============================================

if __name__ == '__main__':
    engine = Engine('D:/_Projects/giaogiao-search-engine/config.ini')
    engine.go()