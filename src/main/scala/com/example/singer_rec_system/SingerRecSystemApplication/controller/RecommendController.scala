package com.example.singer_rec_system.SingerRecSystemApplication.controller

import com.example.singer_rec_system.SingerRecSystemApplication.service.RecommendService
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, RestController}
import org.springframework.beans.factory.annotation.Autowired

@RestController
@RequestMapping(Array("/recommend"))
class RecommendController(@Autowired val recommendService: RecommendService) {
  @RequestMapping(value = Array("/update"), method = Array(RequestMethod.GET))
  def update(): String = {
    recommendService.updateRecommend()
    "Recommendations updated successfully."
  }

  @RequestMapping(value = Array("/get/{userId}"), method = Array(RequestMethod.GET))
  def get(@PathVariable userId: String): Array[String] = {
    recommendService.getRecommend(userId)
  }
}
