package com.example.singer_rec_system.SingerRecSystemApplication

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.CorsRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer


@Configuration class WebConfig extends WebMvcConfigurer {
  override def addCorsMappings(registry: CorsRegistry): Unit = {
    registry.addMapping("/**").allowedOriginPatterns("*") .allowedMethods("GET", "POST", "PUT", "DELETE").allowedHeaders("*").allowCredentials(true)
  }
}

