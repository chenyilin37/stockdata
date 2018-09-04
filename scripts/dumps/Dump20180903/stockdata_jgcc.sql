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
  `lx` int(11) NOT NULL,
  `Count` int(11) DEFAULT NULL COMMENT '机构家数',
  `CGChange` varchar(10) DEFAULT NULL COMMENT '持股变化：减持（-1）、持平（0）、增持（1）',
  `ShareHDNum` decimal(13,0) DEFAULT NULL COMMENT '持股总数',
  `VPosition` decimal(15,2) DEFAULT NULL COMMENT '持股市值',
  `TabRate` decimal(13,2) DEFAULT NULL COMMENT '占总股本比例',
  `LTZB` decimal(13,2) DEFAULT NULL COMMENT '占流通股比例',
  `RateChange` decimal(13,2) DEFAULT NULL COMMENT '变动比例',
  `ClosePrice` decimal(13,2) DEFAULT NULL COMMENT '报告期收盘价',
  `PrevRDate` date DEFAULT NULL,
  `PrevShareHDNum` decimal(13,0) DEFAULT NULL COMMENT '上期持股总数，要计算送股',
  `PrevLTZB` decimal(10,2) DEFAULT NULL COMMENT '上期占流通股比例',
  `ShareHDNumChange` decimal(13,0) DEFAULT NULL COMMENT '持股变动数=本期持股-（上期持股+送转股）',
  `ValueChange` decimal(15,2) DEFAULT NULL COMMENT '持仓变动市值',
  `LTZBChange` decimal(13,2) DEFAULT NULL COMMENT '变动占流通股的比例',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCode`,`RDate`,`lx`),
  KEY `index3` (`RDate`,`lx`,`SCode`),
  KEY `index4` (`EndTradeDate`),
  KEY `index5` (`PrevRDate`)
) ENGINE=InnoDB AUTO_INCREMENT=278816 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-09-03 18:08:07
