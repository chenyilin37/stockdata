DROP TRIGGER IF EXISTS `jgccmx_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `jgccmx_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `jgccmx_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `jgccmx_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `jgccmx_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `jgccmx_INIT_REDIS`;
DROP PROCEDURE IF EXISTS `jgccmx_INIT_BYDATE`;


DELIMITER $

CREATE TRIGGER `jgccmx_AFTER_INSERT` AFTER INSERT ON `jgccmx` FOR EACH ROW
BEGIN
  call jgccmx_UPDATE_REDIS( new.id,
														new.SCode,
														new.RDate,
														new.SHCode,
														new.TypeCode,
														new.indtCode,
														new.ShareHDNum,
														new.Vposition,
														new.TabRate,
														new.TabProRate);
END $

CREATE TRIGGER `jgccmx_AFTER_UPDATE` AFTER UPDATE ON `jgccmx` FOR EACH ROW
BEGIN
   call jgccmx_REMOVE_REDIS(old.id,
														old.SCode,
														old.RDate,
														old.SHCode,
														old.TypeCode,
														old.indtCode,
														old.ShareHDNum,
														old.Vposition,
														old.TabRate,
														old.TabProRate);
	
   call jgccmx_UPDATE_REDIS(new.id,
														new.SCode,
														new.RDate,
														new.SHCode,
														new.TypeCode,
														new.indtCode,
														new.ShareHDNum,
														new.Vposition,
														new.TabRate,
														new.TabProRate);
END $


CREATE TRIGGER `jgccmx_AFTER_DELETE` AFTER DELETE ON `jgccmx` FOR EACH ROW
BEGIN
  call jgccmx_REMOVE_REDIS( old.id,
														old.SCode,
														old.RDate,
														old.SHCode,
														old.TypeCode,
														old.indtCode,
														old.ShareHDNum,
														old.Vposition,
														old.TabRate,
														old.TabProRate);
END $


CREATE PROCEDURE jgccmx_UPDATE_REDIS
(
	in id int,
	in SCode VARCHAR(10),
	in RDate DATE,
	in SHCode VARCHAR(10),
	in TypeCode DECIMAL(8,0),
	in indtCode VARCHAR(10),
	in ShareHDNum DECIMAL(13,0),
	in Vposition DECIMAL(13,0),
	in TabRate DECIMAL(13,2),
	in TabProRate DECIMAL(13,2)
) 
BEGIN
	SET @tableId = 'jgccmx';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'RDate', DATE_FORMAT(RDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'SHCode', SHCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'TypeCode', CAST(TypeCode AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'indtCode', indtCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'ShareHDNum', CAST(ShareHDNum AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'Vposition', CAST(Vposition AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'TabRate', CAST(TabRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'TabProRate', CAST(TabProRate AS CHAR)) INTO @tmp;   


    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHCode), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHCode), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, SHCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode), CAST(id AS CHAR)) INTO @tmp ;

END $


CREATE PROCEDURE jgccmx_REMOVE_REDIS
(
	in id int,
	in SCode VARCHAR(10),
	in RDate DATE,
	in SHCode VARCHAR(10),
	in TypeCode DECIMAL(8,0),
	in indtCode VARCHAR(10),
	in ShareHDNum DECIMAL(13,0),
	in Vposition DECIMAL(13,0),
	in TabRate DECIMAL(13,2),
	in TabProRate DECIMAL(13,2)
) 
BEGIN
  SET @tableId = 'jgccmx';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHCode)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHCode)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, SHCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode)) INTO @tmp ;

END $

CREATE PROCEDURE `jgccmx_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.RDate FROM jgccmx t where t.RDate is not null;
		
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    
	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO mRDate;
		
	   IF done != 1 THEN
	     -- 调用另一个存储过程获取结果
		 	 CALL jgccmx_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'jgccmx',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $


CREATE PROCEDURE `jgccmx_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
   
	DECLARE id int;
	DECLARE SCode VARCHAR(10);
	DECLARE RDate DATE;
	DECLARE SHCode VARCHAR(10);
	DECLARE TypeCode DECIMAL(8,0);
	DECLARE indtCode VARCHAR(10);
	DECLARE ShareHDNum DECIMAL(13,0);
	DECLARE Vposition DECIMAL(13,0);
	DECLARE TabRate DECIMAL(13,2);
	DECLARE TabProRate DECIMAL(13,2);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCode,t.RDate,t.SHCode,t.TypeCode,t.indtCode,t.ShareHDNum,
					 t.Vposition,t.TabRate,t.TabProRate
    FROM jgccmx t where  t.RDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCode,RDate,SHCode,TypeCode,indtCode,ShareHDNum,Vposition,TabRate,TabProRate;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL jgccmx_UPDATE_REDIS(id,SCode,RDate,SHCode,TypeCode,indtCode,ShareHDNum,Vposition,TabRate,TabProRate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END $



DELIMITER ;