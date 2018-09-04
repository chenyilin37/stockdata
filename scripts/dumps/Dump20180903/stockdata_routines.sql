CREATE DATABASE  IF NOT EXISTS `stockdata` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `stockdata`;
-- MySQL dump 10.13  Distrib 5.7.17, for macos10.12 (x86_64)
--
-- Host: 192.168.1.8    Database: stockdata
-- ------------------------------------------------------
-- Server version	5.7.23

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping routines for database 'stockdata'
--
/*!50003 DROP PROCEDURE IF EXISTS `block_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `block_INIT_REDIS`(
   in cacheLevel int
)
    COMMENT '将表中的数据全部同步到Redis'
EXT:BEGIN
    -- 声明局部变量
    DECLARE done boolean default 0;
  
	DECLARE blkucode VARCHAR(10); 
	DECLARE marketid VARCHAR(1); 
	DECLARE blkcode VARCHAR(10); 
	DECLARE blkname VARCHAR(45); 
	DECLARE blktype VARCHAR(10);

	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.blkucode, t.marketid, t.blkcode, t.blkname, t.blktype
 	  FROM block t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
  	IF cacheLevel < 1 THEN
	    leave EXT;
	  END IF;   
   
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO blkucode, marketid, blkcode, blkname, blktype;
		
	    IF done !=1 THEN
	      -- 调用另一个存储过程获取结果
			  CALL block_UPDATE_REDIS(cacheLevel, blkucode, marketid, blkcode, blkname, blktype);
	    END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	-- SELECT redis_hset('CACHED_TABLES', 'block',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `block_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `block_REMOVE_REDIS`(
	in blkucode VARCHAR(10), 
	in marketid VARCHAR(1), 
	in blkcode VARCHAR(10), 
	in blkname VARCHAR(45), 
	in blktype VARCHAR(10)
)
BEGIN
  SELECT redis_srem('blocks', blkucode) INTO @tmp;   

  SET @tableId = 'block';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, blkucode)) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, blkcode)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `block_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `block_UPDATE_REDIS`(
    in cacheLevel int,
	in blkucode VARCHAR(10), 
	in marketid VARCHAR(1), 
	in blkcode VARCHAR(10), 
	in blkname VARCHAR(45), 
	in blktype VARCHAR(10)
)
BEGIN
	SELECT redis_sadd('blocks', blkucode) INTO @tmp;   

	SET @tableId = 'block';
 
    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, blkcode), blkucode) INTO @tmp ;

	-- 缓存数据
    IF cacheLevel > 1 THEN
		-- KEY
		SELECT CONCAT_WS('_', @tableId, blkucode) into @NID;

		-- 字段
		SELECT redis_hset(@NID, 'blkucode', blkucode) INTO @tmp;   
		SELECT redis_hset(@NID, 'marketid', marketid) INTO @tmp;   
		SELECT redis_hset(@NID, 'blkcode', blkcode) INTO @tmp;   
		SELECT redis_hset(@NID, 'blkname', blkname) INTO @tmp;   
		SELECT redis_hset(@NID, 'blktype', blktype) INTO @tmp;   
	END IF;
    

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gdhs_INIT_BYDATE` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gdhs_INIT_BYDATE`(
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
	
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gdhs_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gdhs_INIT_REDIS`(
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
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gdhs_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gdhs_REMOVE_REDIS`(
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

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gdhs_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gdhs_UPDATE_REDIS`(
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

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gudong_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gudong_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  
	DECLARE SHCode VARCHAR(36); 
	DECLARE SHName VARCHAR(36); 
	DECLARE gdlx VARCHAR(36); 
	DECLARE lxdm DECIMAL(8,0); 
	DECLARE niu int; 
	DECLARE VPosition DECIMAL(15,0); 
	DECLARE IndtCode VARCHAR(36); 
	DECLARE IndtName VARCHAR(36);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.SHCode,t.SHName,t.gdlx,t.lxdm,t.niu,t.VPosition,t.IndtCode,t.IndtName
 	  FROM gudong t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO SHCode,SHName,gdlx,lxdm,niu,VPosition,IndtCode,IndtName;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL gudong_UPDATE_REDIS(SHCode,SHName,gdlx,lxdm,niu,VPosition,IndtCode,IndtName);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'gudong',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gudong_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gudong_REMOVE_REDIS`(
	in SHCode VARCHAR(36), 
	in SHName VARCHAR(36), 
	in gdlx VARCHAR(36), 
	in lxdm DECIMAL(8,0), 
	in niu int, 
	in VPosition DECIMAL(15,0), 
	in IndtCode VARCHAR(36), 
	in IndtName VARCHAR(36)
)
BEGIN
  SET @tableId = 'gudong';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, SHCode)) INTO @tmp ;   

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `gudong_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `gudong_UPDATE_REDIS`(
	in SHCode VARCHAR(36), 
	in SHName VARCHAR(36), 
	in gdlx VARCHAR(36), 
	in lxdm DECIMAL(8,0), 
	in niu int, 
	in VPosition DECIMAL(15,0), 
	in IndtCode VARCHAR(36), 
	in IndtName VARCHAR(36)
)
BEGIN
	SET @tableId = 'gudong';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, SHCode) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'SHCode', SHCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHName', SHName) INTO @tmp;   
	SELECT redis_hset(@NID, 'gdlx', gdlx) INTO @tmp;   
	SELECT redis_hset(@NID, 'lxdm', CAST(lxdm AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'niu', CAST(niu AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'VPosition', CAST(VPosition AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'IndtCode', IndtCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'IndtName', IndtName) INTO @tmp; 

    -- 索引
	-- SELECT redis_set( CONCAT_WS('_', @tableId, SHCode), SHCode) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgccmx_INIT_BYDATE` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgccmx_INIT_BYDATE`(
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
	
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgccmx_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgccmx_INIT_REDIS`(
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
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgccmx_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgccmx_REMOVE_REDIS`(
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

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgccmx_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgccmx_UPDATE_REDIS`(
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

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgcc_INIT_BYDATE` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgcc_INIT_BYDATE`(
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
	DECLARE ValueChange DECIMAL(15,2);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCode,t.RDate,t.EndTradeDate,t.lx,t.Count,t.CGChange,
				t.ShareHDNum,t.VPosition,t.TabRate,t.LTZB,t.ShareHDNumChange,t.RateChange,
				t.LTZBChange,t.ClosePrice,t.ValueChange
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
					 LTZBChange,ClosePrice,ValueChange;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL jgcc_UPDATE_REDIS(id,SCode,RDate,EndTradeDate,lx,Count,CGChange,
					 ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange,
					 LTZBChange,ClosePrice,ValueChange);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgcc_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgcc_INIT_REDIS`(
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
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgcc_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgcc_REMOVE_REDIS`(
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
	in ValueChange DECIMAL(15,2)
)
BEGIN
  SET @tableId = 'jgcc';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR))) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR), SCode)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `jgcc_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `jgcc_UPDATE_REDIS`(
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
	in ValueChange DECIMAL(15,2)
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
	SELECT redis_hset(@NID, 'ValueChange', CAST(ChangeValue AS CHAR)) INTO @tmp;   

    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR)), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), CAST(lx AS CHAR), SCode), CAST(id AS CHAR)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `kdaydata_INIT_BYDATE` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `kdaydata_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  
   
  DECLARE id int;
	DECLARE SCode VARCHAR(10);
	DECLARE transDate DATE;
	DECLARE open DECIMAL(10,2);
	DECLARE close DECIMAL(10,2);
	DECLARE high DECIMAL(10,2);
	DECLARE low DECIMAL(10,2);
	DECLARE volume DECIMAL(13,0);
	DECLARE amount DECIMAL(15,2);
	DECLARE LClose DECIMAL(10,2);
	DECLARE changeRate DECIMAL(10,2);
	DECLARE amplitude DECIMAL(10,2);
	DECLARE turnoverRate DECIMAL(10,2);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCode,t.transDate,t.open,t.close,t.high,t.low,
					 t.volume,t.amount,t.LClose,t.changeRate,t.amplitude,t.turnoverRate
    FROM kdaydata t where  t.transDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCode,transDate,open,close,high,low,volume,amount,LClose,changeRate,amplitude,turnoverRate;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL kdaydata_UPDATE_REDIS(id,SCode,transDate,open,close,high,low,volume,amount,LClose,changeRate,amplitude,turnoverRate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `kdaydata_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `kdaydata_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.transDate FROM kdaydata t where t.transDate is not null;
		
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
		 	 CALL kdaydata_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'kdaydata',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `kdaydata_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `kdaydata_REMOVE_REDIS`(
  in id int,
	in SCode VARCHAR(10),
	in transDate DATE,
	in open DECIMAL(10,2),
	in close DECIMAL(10,2),
	in high DECIMAL(10,2),
	in low DECIMAL(10,2),
	in volume DECIMAL(13,0),
	in amount DECIMAL(15,2),
	in LClose DECIMAL(10,2),
	in changeRate DECIMAL(10,2),
	in amplitude DECIMAL(10,2),
	in turnoverRate DECIMAL(10,2)
)
BEGIN
  SET @tableId = 'kdaydata';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(transDate, '%Y-%m-%d'))) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(transDate, '%Y-%m-%d'), SCode)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `kdaydata_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `kdaydata_UPDATE_REDIS`(
  in id int,
	in SCode VARCHAR(10),
	in transDate DATE,
	in open DECIMAL(10,2),
	in close DECIMAL(10,2),
	in high DECIMAL(10,2),
	in low DECIMAL(10,2),
	in volume DECIMAL(13,0),
	in amount DECIMAL(15,2),
	in LClose DECIMAL(10,2),
	in changeRate DECIMAL(10,2),
	in amplitude DECIMAL(10,2),
	in turnoverRate DECIMAL(10,2)
)
BEGIN
	SET @tableId = 'kdaydata';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'transDate', DATE_FORMAT(transDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'open', CAST(open AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'close', CAST(close AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'high', CAST(high AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'low', CAST(low AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'volume', CAST(volume AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'amount', CAST(amount AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LClose', CAST(LClose AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'changeRate', CAST(changeRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'amplitude', CAST(amplitude AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'turnoverRate', CAST(turnoverRate AS CHAR)) INTO @tmp;   


    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(transDate, '%Y-%m-%d')), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(transDate, '%Y-%m-%d'), SCode), CAST(id AS CHAR)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sdltgd_INIT_BYDATE` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `sdltgd_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
   
	DECLARE id int;
	DECLARE SCODE VARCHAR(10);
	DECLARE RDATE DATE;
	DECLARE SHAREHDCODE VARCHAR(45);
	DECLARE SHAREHDNUM DECIMAL(13,0);
	DECLARE LTAG DECIMAL(15,2);
	DECLARE ZB DECIMAL(13,2);
	DECLARE NDATE DATE;
	DECLARE BZ VARCHAR(10);
	DECLARE BDBL DECIMAL(13,2);
	DECLARE SHAREHDNAME VARCHAR(126);
	DECLARE SHAREHDTYPE VARCHAR(10);
	DECLARE SHARESTYPE VARCHAR(10);
	DECLARE RANK DECIMAL(4,0);
	DECLARE SHAREHDRATIO DECIMAL(13,2);
	DECLARE BDSUM DECIMAL(13,2);
	DECLARE COMPANYCODE VARCHAR(45);

    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCODE,t.RDATE,t.SHAREHDCODE,t.SHAREHDNUM,t.LTAG,t.ZB,t.NDATE,t.BZ,
					 t.BDBL,t.SHAREHDNAME,t.SHAREHDTYPE,t.SHARESTYPE,t.RANK,t.SHAREHDRATIO,
					 t.BDSUM,t.COMPANYCODE
    FROM sdltgd t where  t.RDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCODE,RDATE,SHAREHDCODE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,
		 			SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,RANK,SHAREHDRATIO,BDSUM,COMPANYCODE;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL sdltgd_UPDATE_REDIS(id,SCODE,RDATE,SHAREHDCODE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,
		 			SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,RANK,SHAREHDRATIO,BDSUM,COMPANYCODE);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sdltgd_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `sdltgd_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.RDate FROM sdltgd t where t.RDate is not null;
		
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
		 	 CALL sdltgd_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'sdltgd',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sdltgd_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `sdltgd_REMOVE_REDIS`(
	in id int,
	in SCODE VARCHAR(10),
	in RDATE DATE,
	in SHAREHDCODE VARCHAR(45),
	in SHAREHDNUM DECIMAL(13,0),
	in LTAG DECIMAL(15,2),
	in ZB DECIMAL(13,2),
	in NDATE DATE,
	in BZ VARCHAR(10),
	in BDBL DECIMAL(13,2),
	in SHAREHDNAME VARCHAR(126),
	in SHAREHDTYPE VARCHAR(10),
	in SHARESTYPE VARCHAR(10),
	in RANK DECIMAL(4,0),
	in SHAREHDRATIO DECIMAL(13,2),
	in BDSUM DECIMAL(13,2),
	in COMPANYCODE VARCHAR(45)
)
BEGIN
  SET @tableId = 'sdltgd';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHAREHDCODE)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHAREHDCODE)) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, SHAREHDCODE, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sdltgd_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `sdltgd_UPDATE_REDIS`(
	in id int,
	in SCODE VARCHAR(10),
	in RDATE DATE,
	in SHAREHDCODE VARCHAR(45),
	in SHAREHDNUM DECIMAL(13,0),
	in LTAG DECIMAL(15,2),
	in ZB DECIMAL(13,2),
	in NDATE DATE,
	in BZ VARCHAR(10),
	in BDBL DECIMAL(13,2),
	in SHAREHDNAME VARCHAR(126),
	in SHAREHDTYPE VARCHAR(10),
	in SHARESTYPE VARCHAR(10),
	in RANK DECIMAL(4,0),
	in SHAREHDRATIO DECIMAL(13,2),
	in BDSUM DECIMAL(13,2),
	in COMPANYCODE VARCHAR(45)
)
BEGIN
	SET @tableId = 'sdltgd';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'RDate', DATE_FORMAT(RDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'SHAREHDCODE', SHAREHDCODE) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDNUM', CAST(SHAREHDNUM AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LTAG', CAST(LTAG AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'ZB', CAST(ZB AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'NDATE', DATE_FORMAT(NDATE, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'BZ', BZ) INTO @tmp;   
	SELECT redis_hset(@NID, 'BDBL', CAST(BDBL AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDNAME', SHAREHDNAME) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDTYPE', SHAREHDTYPE) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHARESTYPE', SHARESTYPE) INTO @tmp;   
	SELECT redis_hset(@NID, 'RANK', CAST(RANK AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SHAREHDRATIO', CAST(SHAREHDRATIO AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'BDSUM', CAST(BDSUM AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'COMPANYCODE', COMPANYCODE) INTO @tmp;   


    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(RDate, '%Y-%m-%d'), SHAREHDCODE), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode, SHAREHDCODE), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, SHAREHDCODE, DATE_FORMAT(RDate, '%Y-%m-%d'), SCode), CAST(id AS CHAR)) INTO @tmp ;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stocksOfBlock_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stocksOfBlock_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  
	DECLARE blkUcode VARCHAR(10);
	DECLARE stockCode VARCHAR(10);
	DECLARE marketid VARCHAR(2);
   
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.blkUcode, t.stockCode, t.marketid
 	  FROM stocksOfBlock t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO blkUcode, stockCode, marketid;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL stocksOfBlock_UPDATE_REDIS(blkUcode, stockCode, marketid);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'stocksOfBlock',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stocksOfBlock_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stocksOfBlock_REMOVE_REDIS`(
	in blkUcode VARCHAR(10),
	in stockCode VARCHAR(10),
	in marketid VARCHAR(2)
)
BEGIN
 	SELECT redis_srem(CONCAT_WS('_', 'blkstk', blkucode), stockCode) INTO @tmp;   
 	SELECT redis_srem(CONCAT_WS('_', 'stkblk', stockCode), blkucode) INTO @tmp;   

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stocksOfBlock_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stocksOfBlock_UPDATE_REDIS`(
	in blkUcode VARCHAR(10),
	in stockCode VARCHAR(10),
	in marketid VARCHAR(2)
)
BEGIN



 	SELECT redis_sadd(CONCAT_WS('_', 'blkstk', blkucode), stockCode) INTO @tmp;   
 	SELECT redis_sadd(CONCAT_WS('_', 'stkblk', stockCode), blkucode) INTO @tmp;   

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stock_INIT_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stock_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
	 DECLARE stockCode VARCHAR(10);
	 DECLARE stockName VARCHAR(45);
	 DECLARE marketid int;
	 DECLARE stockUcode VARCHAR(10);
	 DECLARE isNew int;
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.stockCode,t.stockName,t.marketid,t.stockUcode,t.isNew
 	  FROM stock t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO stockCode,stockName,marketid,stockUcode,isNew;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL stock_UPDATE_REDIS(stockCode,stockName,marketid,stockUcode,isNew);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'stock',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stock_REMOVE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stock_REMOVE_REDIS`(
 in stockCode VARCHAR(10),
 in stockName VARCHAR(45),
 in marketid int,
 in stockUcode VARCHAR(10),
 in isNew int
)
BEGIN
	SELECT redis_srem('stocks', stockCode) INTO @tmp;   

  SET @tableId = 'stock';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, stockCode)) INTO @tmp ;   

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `stock_UPDATE_REDIS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`chenyl`@`%` PROCEDURE `stock_UPDATE_REDIS`(
 in stockCode VARCHAR(10),
 in stockName VARCHAR(45),
 in marketid int,
 in stockUcode VARCHAR(10),
 in isNew int
)
BEGIN
	 SELECT redis_sadd('stocks', stockCode) INTO @tmp;   

	 SET @tableId = 'stock';

    -- 更新缓存
	 SELECT CONCAT_WS('_', @tableId, stockCode) into @NID;

    -- 字段
	 SELECT redis_hset(@NID, 'stockCode', stockCode) INTO @tmp;   
	 SELECT redis_hset(@NID, 'stockName', stockName) INTO @tmp;   
	 SELECT redis_hset(@NID, 'marketid', CAST(marketid AS CHAR)) INTO @tmp;   
	 SELECT redis_hset(@NID, 'stockUcode', stockUcode) INTO @tmp;   
	 SELECT redis_hset(@NID, 'isNew', CAST(isNew AS CHAR)) INTO @tmp;   

    -- 索引

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-09-03 18:08:07
