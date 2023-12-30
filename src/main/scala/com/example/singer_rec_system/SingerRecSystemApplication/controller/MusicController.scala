package com.example.singer_rec_system.SingerRecSystemApplication.controller

import com.example.singer_rec_system.SingerRecSystemApplication.service.MusicService
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, RestController}
import org.springframework.beans.factory.annotation.Autowired

@RestController
@RequestMapping(Array("/music"))
class MusicController(@Autowired val musicService: MusicService) {
  @RequestMapping(value = Array("/get/{musicname}/{singername}"), method = Array(RequestMethod.GET))
  def get(@PathVariable("musicname") musicname: String, @PathVariable("singername") singername: String): Array[String] = {
    musicService.getMusic(musicname, singername)
  }

  //getLike is to know if user liked the song when searching a song in order to initialize the like checkBox
  @RequestMapping(value = Array("/getLike/{username}/{musicname}/{singername}"), method = Array(RequestMethod.GET))
  def getLike(@PathVariable("username") username: String, @PathVariable("musicname") musicname: String, @PathVariable("singername") singername: String): Boolean = {
    musicService.getMusicLike(username, musicname, singername)
  }

  //alter like and rating value in HBase
  @RequestMapping(value = Array("/like/{username}/{musicname}/{singername}/{isLike}"), method = Array(RequestMethod.GET))
  def like(@PathVariable("username") username: String, @PathVariable("musicname") musicname: String, @PathVariable("singername") singername: String, @PathVariable("isLike") isLike: Boolean): Unit = {
    musicService.likeMusic(username, musicname, singername, isLike)
  }

  //alter play_cnt and rating value in HBase
  @RequestMapping(value = Array("/play/{username}/{musicname}/{singername}"), method = Array(RequestMethod.GET))
  def play(@PathVariable("username") username: String, @PathVariable("musicname") musicname: String, @PathVariable("singername") singername: String): Unit = {
    musicService.playMusic(username, musicname, singername)
  }
}
