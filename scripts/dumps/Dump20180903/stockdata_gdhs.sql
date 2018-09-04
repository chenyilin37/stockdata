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
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`stockCode`,`EndDate`),
  KEY `index3` (`EndDate`,`stockCode`),
  KEY `index4` (`EndTradeDate`)
) ENGINE=InnoDB AUTO_INCREMENT=137355 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-09-03 18:08:05
