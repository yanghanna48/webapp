import logging
logging.basicConfig(level=logging.INFO)
import asyncio,aiomysql,sys,random

def log(sql,args=()):
    logging.info('SQL:%s'%sql)

async def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global __pool
    __pool=await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )



async def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()

class Field(object):
    def __init__(self,name,column_type,primary_key,default):
        self.name=name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return ('<%s,%s,%s>'%(self.__class__.__name__,self.name,self.column_type))

class StringField(Field):
    def __init__(self,name=None,column_type='varchar(100)',primary_key=False,default=None):
        super().__init__(name,column_type,primary_key,default)
class IntegerField(Field):
    def __init__(self,name=None,column_type='int',primary_key=False,default=0):
        super().__init__(name,column_type,primary_key,default)
class BoolField(Field):
    def __init__(self,name,column_type='boolean',primary_key=False,default=None):
        super().__init__(name,column_type,primary_key,default)
class FloatField(Field):
    def __init__(self,name,column_type='float',primary_key=False,default=0.0):
        super().__init__(name,column_type,primary_key,default)
class TextField(Field):
    def __init__(self,name,column_type='text',primary_key=False,default=None):
        super().__init__(name,column_type,primary_key,default)

def create_args_string(num):
    L=[]
    for i in range(num) :
        L.append('?')
    return (','.join(L))

async def select(sql,args,size=None):
    log(sql,args)
    async with __pool.acquire() as conn:
        cur=await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs=await cur.fetchmany(size)
        else:
            rs=await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs
async def execute(sql,args):
    log(sql,args)
    global __pool
    async with __pool.acquire()as conn:
        try:
            cur=await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            affected_line=cur.rowcount
            print('execute:',affected_line)
            await cur.close()
        except BaseException as e:
            raise e
        return affected_line


class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        tableName=attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings=dict()
        primarykey=None
        fields=[]
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info(' found mapping: %s ==> %s' % (k,v))
                mappings[k]=v
                if v.primary_key:
                    logging.info('found primary key %s'%k)
                    if primarykey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primarykey=k
                else:
                    fields.append(k)
        if not primarykey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields=list(map(lambda f: '`%s`'%f,fields))

        attrs['__table__']=tableName
        attrs['__mappings__']=mappings
        attrs['__primary_key__']=primarykey
        attrs['__fields__']=fields
        attrs['__select__']='select`%s`,%s from %s'%(primarykey,','.join(escaped_fields), tableName)
        attrs['__insert__']='insert into `%s`(%s,`%s`)values(%s)'%(tableName,','.join(escaped_fields),primarykey,create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update `%s` set %s where `%s`=?' % (tableName,','.join(map(lambda f:'`%s`=?'%f ,fields)),primarykey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primarykey)
        return type.__new__(cls,name,bases,attrs)


class Model(dict,metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    # 设置自身属性
    # def __setattr__(self, key, value):
        # self[key] = value

    def getValueOrDefault(self,key):
        value= getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,str(value)))
                setattr(self,key,value)
        return value

    def getValue(self,key):
        return getattr(self,key,None)

    @classmethod
    async def findAll(cls,where=None,args=None,**kw):
        sql=[cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        orderBy=kw.get('orderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit=kw.get('limit',None)
        if args is None:
            args=[]
        if limit is not None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple):
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs=await select(' '.join(sql),args)
        return rs
    @classmethod
    async def findNumber(cls,selectField,where=None,args=None):
        sql=['select %s _num_ from `%s`'%(selectField,cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs=await select(' '.join(sql),args,1)
        if len(rs)==0:
            return None
        return rs[0]['_num_']
    @classmethod
    async def find(cls,pk):
        rs =await select('%s where %s=?'%(cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs)==None:
            return None
        return cls(**rs[0])

    async def save(self):
        args=list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows=await execute(self.__insert__,args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args=list(map(self.getValue,self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows=await execute(self.__update__,args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args=[self.getValue(self.__primary_key__)]
        rows=await execute(self.__delete__,args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)



if __name__ == "__main__":
    class User(Model):
        id=IntegerField('id',primary_key=True,)
        name=StringField('name')
        email = StringField('email')
        password = StringField('password')
    loop=asyncio.get_event_loop()


    async def test():
        await create_pool(loop=loop,user='root',password='8023yh',db='webapp')
        user = User(id = random.randint(5,100),name='yangyang',email='xh@pthon.com',password='123456')
        await user.save()
        print('insert test-------：',user)
        r = await User.findAll( where='name=? and password=?',args=['yangyang','123456'],orderBy='id DESC',limit=4) #查询所有记录：测试按条件查询
        print('findAll test-------',r)
        f=await User.findNumber(selectField='id',where='name=?',args=['yangyang'])
        print('findNumber test----------',f)
        user1 = User(id = 77,name='yang',email='xh@qq.com',password='123456') #user1是数据库中id已经存在的一行的新数据
        u = await user1.update() #测试update,传入User实例对象的新数据
        print('update test------',user1)
        d = await user.remove() #测试remove
        print('remove test---------',d)
        s = await User.find(90) #测试find by primary key
        print('find test------',s)
        await destory_pool()


loop.run_until_complete(test())
loop.close()
if loop.is_closed():
    sys.exit(0)
    