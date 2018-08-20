-- MySQL dump 10.13  Distrib 5.5.60, for debian-linux-gnu (armv8l)
--
-- Host: localhost    Database: TestStock
-- ------------------------------------------------------
-- Server version	5.5.60-0+deb8u1

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
-- Table structure for table `block`
--

DROP TABLE IF EXISTS `block`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `block` (
  `blkucode` varchar(10) NOT NULL,
  `marketid` varchar(1) DEFAULT NULL,
  `blkcode` varchar(8) NOT NULL,
  `blkname` varchar(45) NOT NULL,
  `blktype` varchar(8) DEFAULT NULL COMMENT '1-行业\n2-地域\n3-概念',
  PRIMARY KEY (`blkucode`),
  UNIQUE KEY `index2` (`blkcode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `block_AFTER_INSERT` AFTER INSERT ON `block` FOR EACH ROW
BEGIN
  call block_UPDATE_REDIS(new.blkucode, new.marketid, new.blkcode, new.blkname, new.blktype);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `block_AFTER_UPDATE` AFTER UPDATE ON `block` FOR EACH ROW
BEGIN
   call block_REMOVE_REDIS(old.blkucode, old.marketid, old.blkcode, old.blkname, old.blktype);
   call block_UPDATE_REDIS(new.blkucode, new.marketid, new.blkcode, new.blkname, new.blktype);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `block_AFTER_DELETE` AFTER DELETE ON `block` FOR EACH ROW
BEGIN
  call block_REMOVE_REDIS(old.blkucode, old.marketid, old.blkcode, old.blkname, old.blktype);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `gdhs`
--

DROP TABLE IF EXISTS `gdhs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gdhs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stockCode` varchar(10) NOT NULL,
  `EndDate` date NOT NULL COMMENT '统计截止日期',
  `EndTradeDate` date DEFAULT NULL COMMENT '统计截止日期对应的交易日',
  `HolderNum` decimal(13,0) DEFAULT NULL COMMENT '股东数',
  `HolderNumChange` decimal(13,0) DEFAULT NULL COMMENT '股东户数增减',
  `HolderNumChangeRate` decimal(10,2) DEFAULT NULL COMMENT '增减比例',
  `RangeChangeRate` decimal(10,2) DEFAULT NULL COMMENT '期间股价涨跌幅',
  `HolderAvgCapitalisation` decimal(13,2) DEFAULT NULL COMMENT '户均市值',
  `HolderAvgStockQuantity` decimal(13,0) DEFAULT NULL COMMENT '户均持股',
  `TotalCapitalisation` decimal(15,2) DEFAULT NULL COMMENT '总市值',
  `CapitalStock` decimal(13,0) DEFAULT NULL COMMENT '总股本',
  `CapitalStockChange` decimal(13,0) DEFAULT NULL COMMENT '总股本变动',
  `CapitalStockChangeEvent` varchar(45) DEFAULT NULL COMMENT '总股本变动原因',
  `NoticeDate` date DEFAULT NULL COMMENT '公告日期',
  `ClosePrice` decimal(10,2) DEFAULT NULL COMMENT '收盘价',
  `PreviousHolderNum` decimal(13,0) DEFAULT NULL COMMENT '上期股东数',
  `PreviousEndDate` date DEFAULT NULL COMMENT '上期截止日期',
  `temp` decimal(13,2) DEFAULT '0.00',
  `nextClose` decimal(13,2) DEFAULT NULL COMMENT '下季收盘价',
  `nextZF` decimal(13,2) DEFAULT NULL COMMENT '到下季时的涨幅',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`stockCode`,`EndDate`),
  KEY `index3` (`EndDate`,`stockCode`)
) ENGINE=InnoDB AUTO_INCREMENT=27984 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gdhs_AFTER_INSERT` AFTER INSERT ON `gdhs` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gdhs_AFTER_UPDATE` AFTER UPDATE ON `gdhs` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gdhs_AFTER_DELETE` AFTER DELETE ON `gdhs` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `gudong`
--

DROP TABLE IF EXISTS `gudong`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gudong` (
  `SHCode` varchar(36) NOT NULL COMMENT '股东代码',
  `SHName` varchar(128) DEFAULT NULL COMMENT '股东名称',
  `gdlx` varchar(20) DEFAULT NULL COMMENT '股东类型',
  `lxdm` decimal(8,0) DEFAULT NULL COMMENT '1-基金\n2-QFII\n3-社保\n4-券商\n5-保险\n6-信托\n7-金融、（此编号及以下，自编，带确认）\n8-投资公司、\n9-财务公司、\n10-集合理财计划、\n11-个人、\n99-其他',
  `niu` int(11) DEFAULT '0' COMMENT '是否实力机构',
  `VPosition` decimal(15,0) DEFAULT NULL COMMENT '机构市值规模（曾经最大市值）',
  `IndtCode` varchar(36) DEFAULT NULL COMMENT '管理公司编号',
  `IndtName` varchar(126) DEFAULT NULL COMMENT '管理公司名称',
  PRIMARY KEY (`SHCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='股东';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gudong_AFTER_INSERT` AFTER INSERT ON `gudong` FOR EACH ROW
BEGIN
  call gudong_UPDATE_REDIS(new.SHCode,new.SHName,new.gdlx,new.lxdm,new.niu,new.VPosition,new.IndtCode,new.IndtName);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gudong_AFTER_UPDATE` AFTER UPDATE ON `gudong` FOR EACH ROW
BEGIN
   call gudong_REMOVE_REDIS(old.SHCode,old.SHName,old.gdlx,old.lxdm,old.niu,old.VPosition,old.IndtCode,old.IndtName);
   call gudong_UPDATE_REDIS(new.SHCode,new.SHName,new.gdlx,new.lxdm,new.niu,new.VPosition,new.IndtCode,new.IndtName);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `gudong_AFTER_DELETE` AFTER DELETE ON `gudong` FOR EACH ROW
BEGIN
  call gudong_REMOVE_REDIS(old.SHCode,old.SHName,old.gdlx,old.lxdm,old.niu,old.VPosition,old.IndtCode,old.IndtName);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `jgcc`
--

DROP TABLE IF EXISTS `jgcc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jgcc` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(6) NOT NULL,
  `RDate` date NOT NULL COMMENT '统计截止日期',
  `EndTradeDate` date DEFAULT NULL COMMENT '统计截止日期对应的交易日',
  `lx` decimal(10,0) NOT NULL,
  `Count` decimal(13,0) DEFAULT NULL COMMENT '机构家数',
  `CGChange` varchar(10) DEFAULT NULL COMMENT '持股变化：减持（-1）、持平（0）、增持（1）',
  `ShareHDNum` decimal(13,0) DEFAULT NULL COMMENT '持股总数',
  `VPosition` decimal(15,2) DEFAULT NULL COMMENT '持股市值',
  `TabRate` decimal(13,2) DEFAULT NULL COMMENT '占总股本比例',
  `LTZB` decimal(13,2) DEFAULT NULL COMMENT '占流通股比例',
  `ShareHDNumChange` decimal(13,0) DEFAULT NULL COMMENT '持股变动数',
  `RateChange` decimal(13,2) DEFAULT NULL COMMENT '变动比例',
  `LTZBChange` decimal(13,2) DEFAULT NULL COMMENT '变动占流通股的比例',
  `ClosePrice` decimal(13,2) DEFAULT NULL COMMENT '报告期收盘价',
  `ChangeValue` decimal(13,2) DEFAULT NULL COMMENT '持仓变动市值',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCode`,`RDate`,`lx`),
  KEY `index3` (`RDate`,`lx`,`SCode`)
) ENGINE=InnoDB AUTO_INCREMENT=2762 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgcc_AFTER_INSERT` AFTER INSERT ON `jgcc` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgcc_AFTER_UPDATE` AFTER UPDATE ON `jgcc` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgcc_AFTER_DELETE` AFTER DELETE ON `jgcc` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `jgccmx`
--

DROP TABLE IF EXISTS `jgccmx`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jgccmx` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(10) DEFAULT NULL COMMENT '股票代码',
  `RDate` date DEFAULT NULL,
  `SHCode` varchar(10) DEFAULT NULL COMMENT '机构代码',
  `TypeCode` decimal(8,0) DEFAULT NULL COMMENT '机构类型',
  `indtCode` varchar(10) DEFAULT NULL COMMENT '机构管理公司代码',
  `ShareHDNum` decimal(15,0) DEFAULT NULL COMMENT '持股',
  `Vposition` decimal(15,0) DEFAULT NULL COMMENT '市值',
  `TabRate` decimal(10,2) DEFAULT NULL COMMENT '占总股本比例',
  `TabProRate` decimal(10,2) DEFAULT NULL COMMENT '占流通股比例',
  `PrevRDate` date DEFAULT NULL,
  `PrevHDNum` decimal(15,0) DEFAULT '0',
  `PrevVPosition` decimal(15,0) DEFAULT '0',
  `ChangeHDNum` decimal(15,0) DEFAULT '0',
  `ChangeValue` decimal(15,0) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `SCode_UNIQUE` (`SCode`,`RDate`,`SHCode`),
  KEY `index3` (`RDate`,`SCode`,`SHCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='机构持股明细';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgccmx_AFTER_INSERT` AFTER INSERT ON `jgccmx` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgccmx_AFTER_UPDATE` AFTER UPDATE ON `jgccmx` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `jgccmx_AFTER_DELETE` AFTER DELETE ON `jgccmx` FOR EACH ROW
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `kdaydata`
--

DROP TABLE IF EXISTS `kdaydata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kdaydata` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(10) NOT NULL,
  `transDate` date NOT NULL,
  `open` decimal(10,2) DEFAULT NULL,
  `close` decimal(10,2) DEFAULT NULL,
  `high` decimal(10,2) DEFAULT NULL,
  `low` decimal(10,2) DEFAULT NULL,
  `volume` decimal(13,0) DEFAULT NULL,
  `amount` decimal(15,2) DEFAULT NULL,
  `LClose` decimal(10,2) DEFAULT NULL,
  `changeRate` decimal(10,2) DEFAULT NULL,
  `amplitude` decimal(10,2) DEFAULT NULL,
  `turnoverRate` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCode`,`transDate`),
  KEY `index3` (`transDate`,`SCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `kdaydata_AFTER_INSERT` AFTER INSERT ON `kdaydata` FOR EACH ROW
BEGIN
  call kdaydata_UPDATE_REDIS( new.id,
															new.SCode,
															new.transDate,
															new.open,
															new.close,
															new.high,
															new.low,
															new.volume,
															new.amount,
															new.LClose,
															new.changeRate,
															new.amplitude,
															new.turnoverRate);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `kdaydata_AFTER_UPDATE` AFTER UPDATE ON `kdaydata` FOR EACH ROW
BEGIN
   call kdaydata_REMOVE_REDIS(old.id,
															old.SCode,
															old.transDate,
															old.open,
															old.close,
															old.high,
															old.low,
															old.volume,
															old.amount,
															old.LClose,
															old.changeRate,
															old.amplitude,
															old.turnoverRate);
	
   call kdaydata_UPDATE_REDIS(new.id,
															new.SCode,
															new.transDate,
															new.open,
															new.close,
															new.high,
															new.low,
															new.volume,
															new.amount,
															new.LClose,
															new.changeRate,
															new.amplitude,
															new.turnoverRate);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `kdaydata_AFTER_DELETE` AFTER DELETE ON `kdaydata` FOR EACH ROW
BEGIN
  call kdaydata_REMOVE_REDIS( old.id,
															old.SCode,
															old.transDate,
															old.open,
															old.close,
															old.high,
															old.low,
															old.volume,
															old.amount,
															old.LClose,
															old.changeRate,
															old.amplitude,
															old.turnoverRate);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `sdltgd`
--

DROP TABLE IF EXISTS `sdltgd`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sdltgd` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCODE` varchar(10) NOT NULL,
  `RDATE` date DEFAULT NULL,
  `SHAREHDCODE` varchar(45) DEFAULT NULL COMMENT '股东编号，比较长uuid',
  `SHAREHDNUM` decimal(15,0) DEFAULT NULL COMMENT '持股数',
  `LTAG` decimal(15,2) DEFAULT NULL,
  `ZB` decimal(13,2) DEFAULT NULL COMMENT '占比',
  `NDATE` date DEFAULT NULL COMMENT '公告日期',
  `BZ` varchar(45) DEFAULT NULL COMMENT '标志：新进、增加、减少、不变',
  `BDBL` decimal(13,2) DEFAULT NULL COMMENT '股东持股变动占其持仓的比例',
  `SHAREHDNAME` varchar(126) DEFAULT NULL COMMENT '股东名称',
  `SHAREHDTYPE` varchar(45) DEFAULT NULL COMMENT '股东类型',
  `SHARESTYPE` varchar(45) DEFAULT NULL COMMENT '股份类型',
  `RANK` decimal(4,0) DEFAULT NULL COMMENT '排名',
  `SHAREHDRATIO` decimal(13,2) DEFAULT NULL COMMENT '占股东持股比例',
  `BDSUM` decimal(15,2) DEFAULT NULL COMMENT '变动股数',
  `COMPANYCODE` varchar(45) DEFAULT NULL COMMENT '上市公司编号',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCODE`,`RDATE`,`SHAREHDCODE`),
  KEY `index3` (`RDATE`,`SCODE`,`SHAREHDCODE`)
) ENGINE=InnoDB AUTO_INCREMENT=12975 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `sdltgd_AFTER_INSERT` AFTER INSERT ON `sdltgd` FOR EACH ROW
BEGIN
  call sdltgd_UPDATE_REDIS( new.id,
														new.SCODE,
														new.RDATE,
														new.SHAREHDCODE,
														new.SHAREHDNUM,
														new.LTAG,
														new.ZB,
														new.NDATE,
														new.BZ,
														new.BDBL,
														new.SHAREHDNAME,
														new.SHAREHDTYPE,
														new.SHARESTYPE,
														new.RANK,
														new.SHAREHDRATIO,
														new.BDSUM,
														new.COMPANYCODE);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `sdltgd_AFTER_UPDATE` AFTER UPDATE ON `sdltgd` FOR EACH ROW
BEGIN
   call sdltgd_REMOVE_REDIS(old.id,
														old.SCODE,
														old.RDATE,
														old.SHAREHDCODE,
														old.SHAREHDNUM,
														old.LTAG,
														old.ZB,
														old.NDATE,
														old.BZ,
														old.BDBL,
														old.SHAREHDNAME,
														old.SHAREHDTYPE,
														old.SHARESTYPE,
														old.RANK,
														old.SHAREHDRATIO,
														old.BDSUM,
														old.COMPANYCODE);
	
   call sdltgd_UPDATE_REDIS(new.id,
														new.SCODE,
														new.RDATE,
														new.SHAREHDCODE,
														new.SHAREHDNUM,
														new.LTAG,
														new.ZB,
														new.NDATE,
														new.BZ,
														new.BDBL,
														new.SHAREHDNAME,
														new.SHAREHDTYPE,
														new.SHARESTYPE,
														new.RANK,
														new.SHAREHDRATIO,
														new.BDSUM,
														new.COMPANYCODE);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `sdltgd_AFTER_DELETE` AFTER DELETE ON `sdltgd` FOR EACH ROW
BEGIN
  call sdltgd_REMOVE_REDIS( old.id,
														old.SCODE,
														old.RDATE,
														old.SHAREHDCODE,
														old.SHAREHDNUM,
														old.LTAG,
														old.ZB,
														old.NDATE,
														old.BZ,
														old.BDBL,
														old.SHAREHDNAME,
														old.SHAREHDTYPE,
														old.SHARESTYPE,
														old.RANK,
														old.SHAREHDRATIO,
														old.BDSUM,
														old.COMPANYCODE);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `stock`
--

DROP TABLE IF EXISTS `stock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stock` (
  `stockCode` varchar(10) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `marketid` int(11) DEFAULT NULL,
  `stockUcode` varchar(10) DEFAULT NULL,
  `isNew` int(11) DEFAULT NULL COMMENT '新股 ，及N打头的',
  PRIMARY KEY (`stockCode`),
  KEY `index2` (`marketid`,`stockCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stock_AFTER_INSERT` AFTER INSERT ON `stock` FOR EACH ROW
BEGIN
  call stock_UPDATE_REDIS(new.stockCode,new.stockName,new.marketid,new.stockUcode,new.isNew);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stock_AFTER_UPDATE` AFTER UPDATE ON `stock` FOR EACH ROW
BEGIN
   call stock_REMOVE_REDIS(old.stockCode,old.stockName,old.marketid,old.stockUcode,old.isNew);
   call stock_UPDATE_REDIS(new.stockCode,new.stockName,new.marketid,new.stockUcode,new.isNew);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stock_AFTER_DELETE` AFTER DELETE ON `stock` FOR EACH ROW
BEGIN
  call stock_REMOVE_REDIS(old.stockCode,old.stockName,old.marketid,old.stockUcode,old.isNew);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `stockCapital`
--

DROP TABLE IF EXISTS `stockCapital`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stockCapital` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stockCode` varchar(10) DEFAULT NULL,
  `RDate` date DEFAULT NULL,
  `zgb` decimal(15,0) DEFAULT NULL COMMENT '总股本',
  `ltg` decimal(15,0) DEFAULT NULL COMMENT '流通股',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`stockCode`,`RDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stocksOfBlock`
--

DROP TABLE IF EXISTS `stocksOfBlock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stocksOfBlock` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `blkUcode` varchar(10) DEFAULT NULL,
  `stockCode` varchar(10) DEFAULT NULL,
  `marketid` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ndx` (`blkUcode`,`stockCode`),
  KEY `index3` (`stockCode`,`blkUcode`)
) ENGINE=InnoDB AUTO_INCREMENT=12346 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stocksOfBlock_AFTER_INSERT` AFTER INSERT ON `stocksOfBlock` FOR EACH ROW
BEGIN
  call stocksOfBlock_UPDATE_REDIS(new.blkUcode,new.stockCode,new.marketid);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stocksOfBlock_AFTER_UPDATE` AFTER UPDATE ON `stocksOfBlock` FOR EACH ROW
BEGIN
   call stocksOfBlock_REMOVE_REDIS(old.blkUcode,old.stockCode,old.marketid);
   call stocksOfBlock_UPDATE_REDIS(new.blkUcode,new.stockCode,new.marketid);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`chenyl`@`%`*/ /*!50003 TRIGGER `stocksOfBlock_AFTER_DELETE` AFTER DELETE ON `stocksOfBlock` FOR EACH ROW
BEGIN
  call stocksOfBlock_REMOVE_REDIS(old.blkUcode,old.stockCode,old.marketid);
END */;;
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

-- Dump completed on 2018-08-20 10:33:13
