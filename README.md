# giaogiao-search-engine
该项目做为互联网搜索引擎练手项目，使用python实现，搜索引擎叫GiaoGiao。
giaogiao.py中包含四个子模块：
- 下载器（Downloader）
- 转换器（Converter）
- 索引器（Indexer）
- 搜索器（Searcher）

每个模块可独立运行也可配合完成任务，模块启动需要一个配置文件，使用giaogiao内置的Engine模块初始化其他模块，其配置文件格式如下：
```ini
; downloader模块 ==============================================
[downloader]
; 是否需要加载该模块 [true/false]
enable=true
; 目标url表文件路径
targetPath=/target.txt
; 下载后文件夹路径
sourceDir=/data/source
; 文档信息包含文件路径
includePath=/data/include.ini
; 扫描下载网页最大深度
maxDeepth=1
; 允许的最多爬虫线程池容量
spiderPoolSize=32
; 是否需要连接converter模块使用 [true/false]
linkConverter=true

; converter模块 ==============================================
[converter]
; 是否需要加载该模块 [ture/false]
enable=true
; 是否启用交互式运行 [true/false]
interractive=true
; 源网页文件所在文件夹
sourceDir=/data/source
; 处理完之后文档所放文件夹
doneDir=/data/done
; 停用字表文件
stopwordsPath=/stopwords_ch.txt
; 解析器池容量
resolverPoolSize=32
; 是否需要连接indexer使用 [true/false]
linkIndexer=false

; indexer模块 ================================================
[indexer]
; 是否需要加载该模块 [true/false]
enable=false
; 是否启用交互式运行 [true/false]
interractive=false

; seacher模块 ===============================================
[searcher]
; 是否需要加载该模块 [true/false]
enable=false
```
