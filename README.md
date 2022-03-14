# Cloud databases project

**2021/2022** - 5th Year, 1st Semester (Exchange Semester at [TUM](https://www.tum.de/))

**Course:** *Cloud Databases* ([CDB](https://www.in.tum.de/i13/teaching/winter-semester-201920/practical-course-cloud-data-bases/))

**Authors:** David Silva ([daviddias99](https://github.com/daviddias99)), Lukas Bernwald, Krisela Skenderi)

---

**Description:** For the course, we had to develop and eveolve a distributed database. 
- The goal of the 1st milestone was to create a simple echo client, that would connect to a network server. 
- For the 2nd milestone, a basic key-value storage server and client were created. 
- The 3rd milestone extended the system by creating a distributed responsability server ring, where each server was responsible for a given interval of keys. The structure of the ring was mantained by a centralized peer (External Configuration Server) which reacted to server joins, fails and departures.
- The 4th milestone introduced replication to the key-value pairs.
- For the project, we were given the liberty to extend the created system. We chose to remove the ECS by implementing the [Chord](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)) distributed hash table protocol.

**Technologies:** Java, JUnit

**Skills:** System design, object oriented programming, distributed systems design, concurrency, protocol design

**Grade:** 20/20 (converted from 1.0 in [TUM Grading](https://www.ph.tum.de/academics/faq/grading/?language=en))

---
