package com.example.singer_rec_system.SingerRecSystemApplication

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class SingerRecSystemApplication

object SingerRecSystemApplicationRunner {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[SingerRecSystemApplication], args: _*)
  }
}
