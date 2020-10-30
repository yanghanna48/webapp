import asyncio
from aiohttp import web
import logging;logging.basicConfig(level=logging.INFO)

def index(request):
    return web.Response(body=b'<h1>hello</h1>',content_type='text/html')#必须传入content_type否则打开链接会开始下载文件

#法一
# async def init()
#     app=web.Application()
#     app.add_routes([web.get('/',index)])
    # apprunner=web.AppRunner(app)# 构造AppRunner对象
    # # await 用来用来声明程序挂起，比如异步程序执行到某一步时需要等待的时间很长，就将此挂起，去执行其他的异步程序
    # await apprunner.setup()# 调用setup()方法，注意因为源码中这个方法被async修饰，所以前面要加上await，否则报错
    # srv=await loop.create_server(apprunner.server,'127.0.0.1',9000)# 将apprunner的server属性传递进去
    # logging.info('the server started')
    # return srv
# loop=asyncio.get_event_loop()#创建一个loop对象
# loop.run_until_complete(init())#run_until_complete会等待协程（异步函数）init完成再执行下一步
# loop.run_forever()#run_forever() 则会永远阻塞当前线程，直到有人停止了该 event loop 为止
# loop.close()

#法二
def init():
    app=web.Application()
    app.add_routes([web.get('/',index)])
    web.run_app(app,host='127.0.0.1',port=9000)

init()















