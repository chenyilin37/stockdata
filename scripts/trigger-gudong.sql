DROP TRIGGER IF EXISTS `gudong_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `gudong_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `gudong_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `gudong_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `gudong_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `gudong_INIT_REDIS`;



DELIMITER $

CREATE TRIGGER `gudong_AFTER_INSERT` AFTER INSERT ON `gudong` FOR EACH ROW
BEGIN
  call gudong_UPDATE_REDIS(new.SHCode,new.SHName,new.gdlx,new.lxdm,new.niu,new.VPosition,new.IndtCode,new.IndtName);
END $

CREATE TRIGGER `gudong_AFTER_UPDATE` AFTER UPDATE ON `gudong` FOR EACH ROW
BEGIN
   call gudong_REMOVE_REDIS(old.SHCode,old.SHName,old.gdlx,old.lxdm,old.niu,old.VPosition,old.IndtCode,old.IndtName);
   call gudong_UPDATE_REDIS(new.SHCode,new.SHName,new.gdlx,new.lxdm,new.niu,new.VPosition,new.IndtCode,new.IndtName);
END $


CREATE TRIGGER `gudong_AFTER_DELETE` AFTER DELETE ON `gudong` FOR EACH ROW
BEGIN
  call gudong_REMOVE_REDIS(old.SHCode,old.SHName,old.gdlx,old.lxdm,old.niu,old.VPosition,old.IndtCode,old.IndtName);
END $


CREATE PROCEDURE gudong_UPDATE_REDIS
(
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

END $


CREATE PROCEDURE gudong_REMOVE_REDIS
(
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

END $



CREATE PROCEDURE `gudong_INIT_REDIS`()
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
	
END $


DELIMITER ;