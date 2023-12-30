package com.example.singer_rec_system.SingerRecSystemApplication.controller

import com.example.singer_rec_system.SingerRecSystemApplication.service.{MusicService, SingerService}
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, RestController}
import org.springframework.beans.factory.annotation.Autowired

@RestController
@RequestMapping(Array("/singer"))
class SingerController(@Autowired val singerService: SingerService) {
  @RequestMapping(value = Array("/get/{singername}"), method = Array(RequestMethod.GET))
  def get(@PathVariable("singername") singername: String): String = {
    singerService.getSinger(singername)
  }

  //getLike is to know if user followed the singer when searching a singer in order to initialize the like checkBox
  @RequestMapping(value = Array("/getFollow/{username}/{singername}"), method = Array(RequestMethod.GET))
  def getFollow(@PathVariable("username") username: String, @PathVariable("singername") singername: String): Boolean = {
    singerService.getSingerFollow(username, singername)
  }

  //alter like and rating value in HBase
  @RequestMapping(value = Array("/follow/{username}/{singername}/{isFollow}"), method = Array(RequestMethod.GET))
  def follow(@PathVariable("username") username: String, @PathVariable("singername") singername: String, @PathVariable("isFollow") isFollow: Boolean): Unit = {
    singerService.followSinger(username, singername, isFollow)
  }
}
