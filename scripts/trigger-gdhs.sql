DROP TRIGGER IF EXISTS `gdhs_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `gdhs_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `gdhs_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `gdhs_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `gdhs_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `gdhs_INIT_REDIS`;
DROP PROCEDURE IF EXISTS `gdhs_INIT_BYDATE`;


DELIMITER $

CREATE TRIGGER `gdhs_AFTER_INSERT` AFTER INSERT ON `gdhs` FOR EACH ROW
BEGIN
  call gdhs_UPDATE_REDIS( new.id,
													new.stockCode,
													new.EndDate,
													new.EndTradeDate,
													new.HolderNum,
													new.HolderNumChange,
													new.HolderNumChangeRate,
													new.RangeChangeRate,
													new.HolderAvgCapitalisation,
													new.HolderAvgStockQuantity,
													new.TotalCapitalisation,
													new.CapitalStock,
													new.CapitalStockChange,
													new.CapitalStockChangeEvent,
													new.NoticeDate,
													new.ClosePrice,
													new.PreviousHolderNum,
													new.PreviousEndDate);
END $

CREATE TRIGGER `gdhs_AFTER_UPDATE` AFTER UPDATE ON `gdhs` FOR EACH ROW
BEGIN
   call gdhs_REMOVE_REDIS(old.id,
													old.stockCode,
													old.EndDate,
													old.EndTradeDate,
													old.HolderNum,
													old.HolderNumChange,
													old.HolderNumChangeRate,
													old.RangeChangeRate,
													old.HolderAvgCapitalisation,
													old.HolderAvgStockQuantity,
													old.TotalCapitalisation,
													old.CapitalStock,
													old.CapitalStockChange,
													old.CapitalStockChangeEvent,
													old.NoticeDate,
													old.ClosePrice,
													old.PreviousHolderNum,
													old.PreviousEndDate);
   call gdhs_UPDATE_REDIS(new.id,
													new.stockCode,
													new.EndDate,
													new.EndTradeDate,
													new.HolderNum,
													new.HolderNumChange,
													new.HolderNumChangeRate,
													new.RangeChangeRate,
													new.HolderAvgCapitalisation,
													new.HolderAvgStockQuantity,
													new.TotalCapitalisation,
													new.CapitalStock,
													new.CapitalStockChange,
													new.CapitalStockChangeEvent,
													new.NoticeDate,
													new.ClosePrice,
													new.PreviousHolderNum,
													new.PreviousEndDate);
END $


CREATE TRIGGER `gdhs_AFTER_DELETE` AFTER DELETE ON `gdhs` FOR EACH ROW
BEGIN
  call gdhs_REMOVE_REDIS(old.id,
													old.stockCode,
													old.EndDate,
													old.EndTradeDate,
													old.HolderNum,
													old.HolderNumChange,
													old.HolderNumChangeRate,
													old.RangeChangeRate,
													old.HolderAvgCapitalisation,
													old.HolderAvgStockQuantity,
													old.TotalCapitalisation,
													old.CapitalStock,
													old.CapitalStockChange,
													old.CapitalStockChangeEvent,
													old.NoticeDate,
													old.ClosePrice,
													old.PreviousHolderNum,
													old.PreviousEndDate);
END $


CREATE PROCEDURE gdhs_UPDATE_REDIS
(
   in id int,
   in stockCode VARCHAR(10),
   in EndDate DATE,
   in EndTradeDate DATE,
   in HolderNum DECIMAL(13,0),
   in HolderNumChange DECIMAL(13,0),
   in HolderNumChangeRate DECIMAL(13,2),
   in RangeChangeRate DECIMAL(13,2),
   in HolderAvgCapitalisation DECIMAL(13,2),
   in HolderAvgStockQuantity DECIMAL(13,0),
   in TotalCapitalisation DECIMAL(15,2),
   in CapitalStock DECIMAL(13,0),
   in CapitalStockChange DECIMAL(13,0),
   in CapitalStockChangeEvent VARCHAR(40),
   in NoticeDate DATE,
   in ClosePrice DECIMAL(13,2),
   in PreviousHolderNum DECIMAL(13,0),
   in PreviousEndDate DATE
) 
BEGIN
	SET @tableId = 'gdhs';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'stockCode', stockCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'EndDate', DATE_FORMAT(EndDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'EndTradeDate', DATE_FORMAT(EndTradeDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'HolderNum', CAST(HolderNum AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'HolderNumChange', CAST(HolderNumChange AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'HolderNumChangeRate', CAST(HolderNumChangeRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'RangeChangeRate', CAST(RangeChangeRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'HolderAvgCapitalisation', CAST(HolderAvgCapitalisation AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'HolderAvgStockQuantity', CAST(HolderAvgStockQuantity AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'TotalCapitalisation', CAST(TotalCapitalisation AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'CapitalStock', CAST(CapitalStock AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'CapitalStockChange', CAST(CapitalStockChange AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'CapitalStockChangeEvent', CapitalStockChangeEvent) INTO @tmp;   
	SELECT redis_hset(@NID, 'NoticeDate', DATE_FORMAT(NoticeDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'ClosePrice', CAST(ClosePrice AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'PreviousHolderNum', CAST(PreviousHolderNum AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'PreviousEndDate', DATE_FORMAT(PreviousEndDate, '%Y-%m-%d') )  INTO @tmp;    

    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, stockCode, DATE_FORMAT(EndDate, '%Y-%m-%d')), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(EndDate, '%Y-%m-%d'),stockCode), CAST(id AS CHAR)) INTO @tmp ;

END $


CREATE PROCEDURE gdhs_REMOVE_REDIS
(
   in id int,
   in stockCode VARCHAR(10),
   in EndDate DATE,
   in EndTradeDate DATE,
   in HolderNum DECIMAL(13,0),
   in HolderNumChange DECIMAL(13,0),
   in HolderNumChangeRate DECIMAL(13,2),
   in RangeChangeRate DECIMAL(13,2),
   in HolderAvgCapitalisation DECIMAL(13,2),
   in HolderAvgStockQuantity DECIMAL(13,0),
   in TotalCapitalisation DECIMAL(15,2),
   in CapitalStock DECIMAL(13,0),
   in CapitalStockChange DECIMAL(13,0),
   in CapitalStockChangeEvent VARCHAR(40),
   in NoticeDate DATE,
   in ClosePrice DECIMAL(13,2),
   in PreviousHolderNum DECIMAL(13,0),
   in PreviousEndDate DATE
) 
BEGIN
  SET @tableId = 'gdhs';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, stockCode, DATE_FORMAT(EndDate, '%Y-%m-%d'))) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(EndDate, '%Y-%m-%d'),stockCode)) INTO @tmp ;

END $

CREATE PROCEDURE `gdhs_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mEndDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.EndDate FROM gdhs t where t.EndDate is not null;
		
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    
	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO mEndDate;
		
	     IF done != 1 THEN
	    -- 调用另一个存储过程获取结果
			CALL gdhs_INIT_BYDATE(mEndDate);
	     END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'gdhs',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $


CREATE PROCEDURE `gdhs_INIT_BYDATE`(
	in mEndDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
   
   DECLARE id int;
   DECLARE stockCode VARCHAR(10);
   DECLARE EndDate DATE;
   DECLARE EndTradeDate DATE;
   DECLARE HolderNum DECIMAL(13,0);
   DECLARE HolderNumChange DECIMAL(13,0);
   DECLARE HolderNumChangeRate DECIMAL(13,2);
   DECLARE RangeChangeRate DECIMAL(13,2);
   DECLARE HolderAvgCapitalisation DECIMAL(13,2);
   DECLARE HolderAvgStockQuantity DECIMAL(13,0);
   DECLARE TotalCapitalisation DECIMAL(15,2);
   DECLARE CapitalStock DECIMAL(13,0);
   DECLARE CapitalStockChange DECIMAL(13,0);
   DECLARE CapitalStockChangeEvent VARCHAR(40);
   DECLARE NoticeDate DATE;
   DECLARE ClosePrice DECIMAL(13,2);
   DECLARE PreviousHolderNum DECIMAL(13,0);
   DECLARE PreviousEndDate DATE;
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id, t.stockCode, t.EndDate, t.EndTradeDate, t.HolderNum, t.HolderNumChange, t.
				HolderNumChangeRate, t.RangeChangeRate, t.HolderAvgCapitalisation, t.
				HolderAvgStockQuantity, t.TotalCapitalisation, t.CapitalStock, t.CapitalStockChange, t.
				CapitalStockChangeEvent, t.NoticeDate, t.ClosePrice, t.PreviousHolderNum, t.PreviousEndDate
 	    FROM gdhs t where  t.EndDate=mEndDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,stockCode,EndDate,EndTradeDate,HolderNum,HolderNumChange,
				HolderNumChangeRate,RangeChangeRate,HolderAvgCapitalisation,
				HolderAvgStockQuantity,TotalCapitalisation,CapitalStock,CapitalStockChange,
				CapitalStockChangeEvent,NoticeDate,ClosePrice,PreviousHolderNum,PreviousEndDate;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL gdhs_UPDATE_REDIS(id,stockCode,EndDate,EndTradeDate,HolderNum,HolderNumChange,
					HolderNumChangeRate,RangeChangeRate,HolderAvgCapitalisation,
					HolderAvgStockQuantity,TotalCapitalisation,CapitalStock,CapitalStockChange,
					CapitalStockChangeEvent,NoticeDate,ClosePrice,PreviousHolderNum,PreviousEndDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END $



DELIMITER ;