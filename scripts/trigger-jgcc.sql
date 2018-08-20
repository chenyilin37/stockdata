DROP TRIGGER IF EXISTS `jgcc_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `jgcc_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `jgcc_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `jgcc_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `jgcc_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `jgcc_INIT_REDIS`;
DROP PROCEDURE IF EXISTS `jgcc_INIT_BYDATE`;


DELIMITER $

CREATE TRIGGER `jgcc_AFTER_INSERT` AFTER INSERT ON `jgcc` FOR EACH ROW
BEGIN
  call jgcc_UPDATE_REDIS( new.id,
													new.SCode,
													new.RDate,
													new.EndTradeDate,
													new.lx,
													new.Count,
													new.CGChange,
													new.ShareHDNum,
													new.VPosition,
													new.TabRate,
													new.LTZB,
													new.ShareHDNumChange,
													new.RateChange,
													new.LTZBChange,
													new.ClosePrice,
													new.ChangeValue);
END $

CREATE TRIGGER `jgcc_AFTER_UPDATE` AFTER UPDATE ON `jgcc` FOR EACH ROW
BEGIN
   call jgcc_REMOVE_REDIS(old.id,
													old.SCode,
													old.RDate,
													old.EndTradeDate,
													old.lx,
													old.Count,
													old.CGChange,
													old.ShareHDNum,
													old.VPosition,
													old.TabRate,
													old.LTZB,
													old.ShareHDNumChange,
													old.RateChange,
													old.LTZBChange,
													old.ClosePrice,
													old.ChangeValue
												);
   call jgcc_UPDATE_REDIS(new.id,
													new.SCode,
													new.RDate,
													new.EndTradeDate,
													new.lx,
													new.Count,
													new.CGChange,
													new.ShareHDNum,
													new.VPosition,
													new.TabRate,
													new.LTZB,
													new.ShareHDNumChange,
													new.RateChange,
													new.LTZBChange,
													new.ClosePrice,
													new.ChangeValue);
END $


CREATE TRIGGER `jgcc_AFTER_DELETE` AFTER DELETE ON `jgcc` FOR EACH ROW
BEGIN
  call jgcc_REMOVE_REDIS(	old.id,
													old.SCode,
													old.RDate,
													old.EndTradeDate,
													old.lx,
													old.Count,
													old.CGChange,
													old.ShareHDNum,
													old.VPosition,
													old.TabRate,
													old.LTZB,
													old.ShareHDNumChange,
													old.RateChange,
													old.LTZBChange,
													old.ClosePrice,
													old.ChangeValue
												);
END $


CREATE PROCEDURE jgcc_UPDATE_REDIS
(
	in id int,
	in SCode VARCHAR(6),
	in RDate DATE,
	in EndTradeDate DATE,
	in lx DECIMAL(10,0),
	in Count DECIMAL(10,0),
	in CGChange VARCHAR(6),
	in ShareHDNum DECIMAL(13,0),
	in VPosition DECIMAL(15,2),
	in TabRate DECIMAL(13,2),
	in LTZB DECIMAL(13,2),
	in ShareHDNumChange DECIMAL(13,0),
	in RateChange DECIMAL(13,2),
	in LTZBChange DECIMAL(13,2),
	in ClosePrice DECIMAL(13,2),
	in ChangeValue DECIMAL(13,2)
) 
BEGIN
	SET @tableId = 'jgcc';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'RDate', DATE_FORMAT(RDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'EndTradeDate', DATE_FORMAT(EndTradeDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'lx', CAST(lx AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'Count', CAST(Count AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'CGChange', CGChange) INTO @tmp;   
	SELECT redis_hset(@NID, 'ShareHDNum', CAST(ShareHDNum AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'VPosition', CAST(VPosition AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'TabRate', CAST(TabRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LTZB', CAST(LTZB AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'ShareHDNumChange', CAST(ShareHDNumChange AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'RateChange', CAST(RateChange AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LTZBChange', CAST(LTZBChange AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'ClosePrice', CAST(ClosePrice AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'ChangeValue', CAST(ChangeValue AS CHAR)) INTO @tmp;   

    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR)), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR), SCode), CAST(id AS CHAR)) INTO @tmp ;

END $


CREATE PROCEDURE jgcc_REMOVE_REDIS
(
	in id int,
	in SCode VARCHAR(6),
	in RDate DATE,
	in EndTradeDate DATE,
	in lx DECIMAL(10,0),
	in Count DECIMAL(10,0),
	in CGChange VARCHAR(6),
	in ShareHDNum DECIMAL(13,0),
	in VPosition DECIMAL(15,2),
	in TabRate DECIMAL(13,2),
	in LTZB DECIMAL(13,2),
	in ShareHDNumChange DECIMAL(13,0),
	in RateChange DECIMAL(13,2),
	in LTZBChange DECIMAL(13,2),
	in ClosePrice DECIMAL(13,2),
	in ChangeValue DECIMAL(13,2)
) 
BEGIN
  SET @tableId = 'jgcc';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR))) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR), SCode)) INTO @tmp ;

END $

CREATE PROCEDURE `jgcc_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.RDate FROM jgcc t where t.RDate is not null;
		
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
		 	 CALL jgcc_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'jgcc',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $


CREATE PROCEDURE `jgcc_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
   
	DECLARE id int;
	DECLARE SCode VARCHAR(6);
	DECLARE RDate DATE;
	DECLARE EndTradeDate DATE;
	DECLARE lx DECIMAL(10,0);
	DECLARE Count DECIMAL(10,0);
	DECLARE CGChange VARCHAR(6);
	DECLARE ShareHDNum DECIMAL(13,0);
	DECLARE VPosition DECIMAL(15,2);
	DECLARE TabRate DECIMAL(13,2);
	DECLARE LTZB DECIMAL(13,2);
	DECLARE ShareHDNumChange DECIMAL(13,0);
	DECLARE RateChange DECIMAL(13,2);
	DECLARE LTZBChange DECIMAL(13,2);
	DECLARE ClosePrice DECIMAL(13,2);
	DECLARE ChangeValue DECIMAL(13,2);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCode,t.RDate,t.EndTradeDate,t.lx,t.Count,t.CGChange,
				t.ShareHDNum,t.VPosition,t.TabRate,t.LTZB,t.ShareHDNumChange,t.RateChange,
				t.LTZBChange,t.ClosePrice,t.ChangeValue
    FROM jgcc t where  t.RDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCode,RDate,EndTradeDate,lx,Count,CGChange,
					 ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange,
					 LTZBChange,ClosePrice,ChangeValue;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL jgcc_UPDATE_REDIS(id,SCode,RDate,EndTradeDate,lx,Count,CGChange,
					 ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange,
					 LTZBChange,ClosePrice,ChangeValue);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END $



DELIMITER ;