__author__ = 'Ayayaneru'

# *** day03 begin ***



import asyncio, logging, aiomysql
import random,sys
logging.basicConfig(level=logging.INFO)#日志记录


# ############创建连接池###########
#日志打印函数：打印出使用的sql语句
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 异步IO起手式 async ，创建连接池函数， pool 用法见下：
# https://aiomysql.readthedocs.io/en/latest/pool.html?highlight=create_pool
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 声明 __pool 为全局变量
    global __pool
    __pool = await aiomysql.create_pool(
        #kw.get(key,default)：通过key在kw中查找对应的value，如果没有则返回默认值default
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],#不设默认值，不需要使用get方法
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

# 协程：销毁所有的数据库连接池
async def destory_pool():
    global  __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()

##############执行 SELECT 语句############
#协程：面向sql的查询操作:size指定返回的查询结果数
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.acquire() as conn:
        #查询需要返回查询的结果，按照dict返回，所以游标cursor中传入了参数aiomysql.DictCursor
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or())
        # SQL 语句的参数占位符是?，而 python 的占位符是%s，坚持使用带参数的 SQL（args），而不是自己拼接 SQL 字符串，这样可以防止 SQL 注入攻击。
        if size:
            # 通过fetchmany()获取最多指定数量的记录，否则，通过fetchall()获取所有记录。
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        #返还查询的结果集
        return rs

##############执行 INSERT、UPDATE、DELETE语句##############
#这三种语句都会影响行数
async def execute(sql, args):#建立连接，游标，执行sql语句，返回！！！受影响的函数！！！
    log(sql,args)
    global __pool
    async with __pool.acquire() as conn :
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'),args)#使用args，防止sql注入
            # rowcount 获取行数，应该表示的是该函数影响的行数
            affected_line = cur.rowcount
            print('execute:',affected_line)
            await cur.close()
        except BaseException as e:
            raise e
        # 返回受影响的行数
        return affected_line

###准备
#目的：查询字段计数：替换成sql识别的'？' 样例： insert into tablename values (?,?,?,?)
#根据输入的字段生成占位符列表
def create_args_string(num):
    L = []
    for i in range(num):
        L.append('?')
    #用，将占位符？拼接起来
    return (','.join(L))


###############定义Field类##############
# 保存数据库中表的字段名和字段类型
class Field(object):
    #表的字段包括：名字、类型、是否为主键、默认值
    def __init__(self,name,column_type,primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    #打印数据库中的表时，输出表的信息：表名（即类名如StringField）、字段名、字段类型
    def __str__(self):#当使用print输出对象的时候，只要自己定义了__str__(self)方法，那么就会打印从在这个方法中return的数据
        return ('<%s,%s,%s>' %(self.__class__.__name__,self.name,self.column_type))

######义不同类型的衍生Field
#表的不同列的字段的类型不同
class StringField(Field):
    def __init__(self,name=None,column_type='varchar(100)',primary_key=False,default=None):
        super().__init__(name,column_type,primary_key,default)
    #Boolean不能做主键
class BooleanField(Field):
    def __init__(self,name=None,default=False):
        super().__init__(name,'Boolean',False,default)

class IntegerField(Field):
    def __init__(self,name=None,primary_key=False,default=0):
        super().__init__(name,'int',primary_key,default)

class FloatField(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,'float',primary_key,default)

class TextField(Field):
    def __init__(self,name=None,default=None):
        super().__init__(name,'text',False,default)

#定义Model的metaclass元类
#所有的元类都继承自type
#ModelMetaclass元类定义了所有Model基类（继承ModelMetaclass）的子类(Model通过条件判断被排除，此案例中的子类是User）实现的操作

# -*-ModelMetaclass：为一个数据库表映射成一个封装的类做准备
# 读取具体子类(eg：user)的映射信息
#创造类的时候，排除对Model类的修改
#在当前类中查找所有的类属性(attrs),如果找到Field属性，就保存在__mappings__的dict里，
#同时从类属性中删除Field（防止实例属性覆盖类的同名属性）
#__table__保存数据库表名
class ModelMetaclass(type):
    #__new__控制__init__的执行，所以在其执行之前执行，且必须有返回值
    # __new__()方法接收到的参数依次是：
    # cls：代表要__init__的类，此参数在实例化时由python解释器自动提供（eg：下文的User、Model)
    # name：类的名字 str
    # bases：类继承的父类集合 Tuple
    # attrs：类的方法集合
    def __new__(cls, name, bases, attrs):
        # print(name)会输出
        #排除对Model的修改，后面全是针对User的
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)#Create and return a new object
        # 获取 table 名称:有表名则为表名，否则None or name则直接为类名
        tableName = attrs.get('__table__', None) or name
        print(tableName)
        # 日志：找到名为 name 的 model
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取Field 和主键名
        mappings = dict()#保存Field属性和列的映射关系
        fields = []#保存非主键的属性名
        primaryKey = None#保存主键的属性名
        #k:类的属性（字段名）；v：数据库表中对应的Field属性
        for k,v in attrs.items():
            # print(' ----------',k,v)#可查看
            #attrs中含有非Field属性的，首先应判断是否是Field属性
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k,v))
                mappings[k] = v#将映保存入mappings
                # 这里的 v.primary_key 我理解为 ：只要 primary_key 为 True 则这个 field 为主键
                if v.primary_key:
                    logging.info('found primary key %s'%k)
                    #主键只有一个，不能多次赋值，返回一个错误
                    if primaryKey:#如果primaryKey已经存在
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    # k设为主键
                    primaryKey = k
                else:
                    #非主键，一律放在fields
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
        #从类属性中删除Field属性
            attrs.pop(k)

        #保存非主键属性为字符串列表形式
        #将非主键属性变成`id`,`name`这种形式（带反引号）
        #repr函数和反引号：取得对象的规范字符串表示
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # print('---------',escaped_fields)

        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName # table 名
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的 SELECT, INSERT, UPDAT E和 DELETE 语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        # attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName,','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)#UPDATE 表名称 SET 列名称 = 新值 WHERE 主键列名称 = 某值
        attrs['__update__']='update `%s` set %s where `%s`=?' % (tableName,','.join(map(lambda f:'`%s`=?'%f ,fields)),primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
# 任何继承自Model的类（比如User），会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性如__table__、__mappings__中

#############定义ORM所有映射的基类：Model##############
#Model类的任意子类可以映射一个数据库表
#Model类可以看做是对所有数据库表操作的基本定义的映射
#基于字典查询形式
#Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__,能够实现属性操作
#实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super().__init__(**kw)#继承父类的init方法
    # __getattr__为内置方法，当使用点号获取实例属性时，如果属性不存在就自动调用__getattr__方法，如后面的self.__mappins__
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    # __setattr__当设置类实例属性时自动调用，如j.name=5 就会调用__setattr__方法 self.[name]=5
    # def __setattr__(self, key, value):
    #     self[key] = value
    # 通过属性返回想要的值
    def getValue(self, key):
        return getattr(self, key, None)#getattr是一个BIF，相当于返回self.key，key不存在时返回default：None
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # 如果 value 为 None，定位某个键； value 不为 None 就直接返回
            field = self.__mappings__[key]
            if field.default is not None:
                # 如果 field.default 不是 None ： 就把它赋值给 value
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,str(value)))
                setattr(self, key, value)
        return value

    # *** 往 Model 类添加 class 方法，就可以让所有子类调用 class 方法
    @classmethod
    async def findAll(cls, where=None,args=None, **kw):
    #样例'select * from user where name=? and password=? order by id DECS limit ?,?'
        sql = [cls.__select__]
        #where 条件过滤
        # where 默认值为 None
        # 如果 where 有值就在 sql 加上字符串 'where' 和 变量 where
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:#确定args为list，为后面limit传参做准备
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            # get 可以返回 orderBy 的值，如果失败就返回 None ，这样失败也不会出错
            # oederBy 有值时给 sql 加上它，为空值时什么也不干
            sql.append('order by')
            sql.append(orderBy)
        # 开头和上面 orderBy 类似
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                # 如果 limit 为整数
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                # 如果 limit 是元组且里面只有两个元素
                sql.append('?, ?')
                # extend 把 limit的值传入空[]
                args.extend(limit)#append是将数加入列表，extend可以将一个iterable对象如tuple加入list
            else:
                # 不行就报错
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)#' '.join(sql)将list转为str，args为limit的参数
        # 返回选择的列表里的所有值 ，完成 findAll 函数
        #**r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        #查询selectField的值，样例'select id from user where name=? 
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)#只返回一个
        if len(rs) == 0:
            # 如果 rs 内无元素，返回 None ；有元素就返回某个数
            return None
        #若直接返回rs，由select函数可知，返回的是这种形式的[{'_num_': 5},...],而我们只需要得到一个数值
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ## find object by primary key
        # 通过主键找对象
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)#pk是select方法的参数，对应主键值的占位符，1是select的位置参数size
        if len(rs) == 0:
            return None
        return cls(**rs[0])
########上面是类方法,应当使用类名+方法名调用，如User.find(),后面为实例方法采用实例+方法名调用，如user1.remove()
    # *** 往 Model 类添加实例方法，就可以让所有子类调用实例方法
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        #args为list（id,name,email,password各属性的值)
        rows = await execute(self.__insert__, args)#execute返回的是受影响的函数
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)
    # save , update , remove 这三个可以对比着来看

if __name__ == '__main__':
    class User(Model):
        __table__='users' 
        #定义类的属性到列的映射：保证通过方法创建的实例的属性与数据库各列要求一致
        id = IntegerField('id',primary_key=True)
        name = StringField('name')
        email = StringField('email')
        password = StringField('password')
    #创建异步事件的句柄
    loop = asyncio.get_event_loop()

    #创建实例
    
    async def test():
        await create_pool(loop=loop,user='root',password='8023yh',db='webapp')#必须先在mysql中create database webapp；create table user(id int,name varchar(100),……);
        user = User(id = random.randint(5,100),name='yangyang',email='xh@pthon.com',password='123456')
        await user.save() #插入一条记录：测试insert
        print('insert test-------：',user)
        #这里可以使用User.findAll()是因为：用@classmethod修饰了Model类里面的findAll()
        #一般来说，要使用某个类的方法，需要先实例化一个对象再调用方法
        #而使用@staticmethod或@classmethod，就可以不需要实例化，直接类名.方法名()来调用
        # r = await User.findAll( where='name=\'yangyang\'',orderBy='id DESC',limit=4) #查询所有记录：测试按条件查询
        r = await User.findAll( where='name=? and password=?',args=['yangyang','123456'],orderBy='id DESC',limit=(2,4)) #查询所有记录：测试按条件查询
        print('findAll test-------',r)
        f=await User.findNumber(selectField='id',where='name=?',args=['yangyang'])
        print('findNumber test----------',f)
        user1 = User(id = 61,name='yang',email='xh@qq.com',password='123456') #user1是数据库中id已经存在的一行的新数据
        u = await user1.update() #测试update,传入User实例对象的新数据
        print('update test------',user1)
        # d = await user.remove() #测试remove
        # print('remove test---------',d)
        s = await User.find(61) #测试find by primary key
        print('find test------',s)
        await destory_pool() #关闭数据库连接池

    loop.run_until_complete(test())
    loop.close()
    if loop.is_closed():
        sys.exit(0)