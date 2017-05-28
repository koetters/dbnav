-- phpMyAdmin SQL Dump
-- version 4.5.4.1deb2ubuntu2
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: May 28, 2017 at 10:38 PM
-- Server version: 5.7.18-0ubuntu0.16.04.1
-- PHP Version: 7.0.15-0ubuntu0.16.04.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `Literature`
--

-- --------------------------------------------------------

--
-- Table structure for table `Author`
--

CREATE TABLE `Author` (
  `name` varchar(85) NOT NULL,
  `nationality` varchar(85) NOT NULL,
  `date_of_birth` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `Author`
--

INSERT INTO `Author` (`name`, `nationality`, `date_of_birth`) VALUES
('Dan Brown', 'American', '1964-06-22'),
('Douglas Adams', 'British', '1952-03-11'),
('J. K. Rowling', 'British', '1965-07-31'),
('Lewis Carroll', 'British', '1832-01-27'),
('Stephen King', 'American', '1947-09-21'),
('Virginia Woolf', 'British', '1882-01-25');

-- --------------------------------------------------------

--
-- Table structure for table `Book`
--

CREATE TABLE `Book` (
  `title` varchar(85) NOT NULL,
  `author` varchar(85) DEFAULT NULL,
  `publication_date` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `Book`
--

INSERT INTO `Book` (`title`, `author`, `publication_date`) VALUES
('Alice in Wonderland', 'Lewis Carroll', '1865-11-26'),
('Doctor Sleep', 'Stephen King', '2013-09-24'),
('Harry Potter and the Deathly Hallows', 'J. K. Rowling', '2007-07-21'),
('Inferno', 'Dan Brown', '2013-03-14'),
('The Casual Vacancy', 'J. K. Rowling', '2012-09-27'),
('The Da Vinci Code', 'Dan Brown', '2003-03-18'),
('The Hitchhiker\'s Guide to the Galaxy', 'Douglas Adams', '1979-10-12'),
('The Shining', 'Stephen King', '1977-01-28'),
('To the Lighthouse', 'Virginia Woolf', '1927-05-05');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `Author`
--
ALTER TABLE `Author`
  ADD PRIMARY KEY (`name`);

--
-- Indexes for table `Book`
--
ALTER TABLE `Book`
  ADD PRIMARY KEY (`title`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
