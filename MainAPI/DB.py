import psycopg2 
from psycopg2.extras import RealDictCursor
import os
# PGSQL TO SQLITE 변환할지 고민중. 과연 PGSQL까지 필요할까?
# 일단, 팀원들에게는 blackbox로 함수를 제공하자. 
# 그편이 수정등에 용이할 것으로 보인다.
# 어차피 코딩을 모르는 상황이라서, SQL쪽은 내가 전임하는 것으로 하자.
# 그렇다면, 어떤 제공을 해야하는가? 명세서 작성이 중요할듯.
# 일단, mocule 단계에서 DB를 응용케하고, 이 함수에서는 DB를 불러오는데 의의를 다하자.

class DB:
    setting = {
        'dbname': os.environ['DB_NAME'],
        'username': os.environ['DB_USER'],
        'password': os.environ['DB_PASS'],
        'host': os.environ['DB_HOST'],
        'port': os.environ['DB_PORT']
    }
    def execute_query(self, query, args={}):
        db = psycopg2.connect(
            dbname=self.setting['dbname'], user=self.setting['username'], password=self.setting['password'], host=self.setting['host'], port=self.setting['port'])
        try:
            cursor = db.cursor(cursor_factory=RealDictCursor)
        except :
            raise Exception('error DB connection!')
        try:
            cursor.execute(query, args)
            try  :
                row = cursor.fetchall()
            except :
                row = 0 
            db.commit() 
        except Exception as e:
            db.rollback()  
            print(query)
            print(args)
            print(e) 
            raise Exception('error DB query! \n *query :' +query+'\n *arguments : '+str(args)+'\n *ERROR : ')
        finally: 
            cursor.close() 
            db.close()
        return row
    
    # 대규모 업데이트시 용하는 함수. 
    def update_object(self,table_name:str,index_key:str,update:dict) :
        query = 'UPDATE ' + table_name + ' SET '

        ct = 0
        v = len(update.keys())
        for key in update.keys() :
            ct += 1
            if key == index_key : continue
            query += key + '= %{'+key+'}s'
            if ct == v : break
            else : query += ', '

        query += ' WHERE ' +  index_key + ' = %{'+index_key+'}s;'
        return self.execute_query(query,update)
        

    