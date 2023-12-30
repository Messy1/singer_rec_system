package com.example.singer_rec_system.SingerRecSystemApplication.controller

import com.example.singer_rec_system.SingerRecSystemApplication.service.UserService
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, RestController}
import org.springframework.beans.factory.annotation.Autowired

@RestController
@RequestMapping(Array("/user"))
class UserController(@Autowired userService: UserService) {
  @RequestMapping(value = Array("/signUp/{username}/{password}"), method = Array(RequestMethod.GET))
  def signUp(@PathVariable("username") username: String, @PathVariable("password") password: String): Boolean = {
    userService.signUp(username, password)
  }

  @RequestMapping(value = Array("/logIn/{username}/{password}"), method = Array(RequestMethod.GET))
  def logIn(@PathVariable("username") username: String, @PathVariable("password") password: String): Boolean = {
    userService.logIn(username, password)
  }
}
