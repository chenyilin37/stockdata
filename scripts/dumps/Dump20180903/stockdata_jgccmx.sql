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
  `TypeCode` int(11) DEFAULT NULL COMMENT '机构类型',
  `indtCode` varchar(10) DEFAULT NULL COMMENT '机构管理公司代码',
  `ShareHDNum` decimal(15,0) DEFAULT NULL COMMENT '持股',
  `Vposition` decimal(15,0) DEFAULT NULL COMMENT '市值',
  `TabRate` decimal(10,2) DEFAULT NULL COMMENT '占总股本比例',
  `TabProRate` decimal(10,2) DEFAULT NULL COMMENT '占流通股比例',
  `ClosePrice` decimal(10,2) DEFAULT NULL,
  `PrevRDate` date DEFAULT NULL,
  `PrevHDNum` decimal(15,0) DEFAULT '0',
  `PrevVPosition` decimal(15,0) DEFAULT '0',
  `PrevLTZB` decimal(10,2) DEFAULT '0.00',
  `ChangeHDNum` decimal(15,0) DEFAULT '0',
  `ChangeValue` decimal(15,0) DEFAULT '0',
  `ChangeLTZB` decimal(10,2) DEFAULT '0.00',
  PRIMARY KEY (`id`),
  UNIQUE KEY `SCode_UNIQUE` (`SCode`,`RDate`,`SHCode`),
  KEY `index3` (`RDate`,`SCode`,`SHCode`),
  KEY `index4` (`ClosePrice`),
  KEY `index5` (`PrevRDate`)
) ENGINE=InnoDB AUTO_INCREMENT=1877982 DEFAULT CHARSET=utf8 COMMENT='机构持股明细';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-09-03 18:08:06
