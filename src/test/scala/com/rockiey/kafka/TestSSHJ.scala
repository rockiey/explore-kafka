package com.rockiey.kafka

import java.io.{BufferedReader, InputStream}

import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.common.IOUtils
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.io.Codec

object TestSSHJ {
  def main(args: Array[String]): Unit = {
    val ssh = new SSHClient()
    ssh.loadKnownHosts()

    ssh.connect("localhost")
    try {
      val userName = System.getProperty("user.name")
      ssh.authPublickey(userName)
      val session = ssh.startSession()

      val input = session.getInputStream
      val output = session.getOutputStream

//      output.write("ping -c 3 google.com".getBytes)
//      output.flush()
//      System.out.println(IOUtils.readFully(input).toString())

      try {
        val cmd = session.exec("ping -c 10 -i 1 google.com")
        val src = scala.io.Source.fromInputStream(cmd.getInputStream)
//        while (src.hasNext) {
//          src.
//        }
//
        echo(cmd.getInputStream)
        System.out.println(scala.io.Source.fromInputStream(cmd.getInputStream()).mkString)
//        System.out.println(IOUtils.readFully(cmd.getInputStream()).toString())
        cmd.join(5, TimeUnit.SECONDS)
        System.out.println("\n** exit status: " + cmd.getExitStatus())
      } finally {
        session.close()
      }
    } finally {
      ssh.disconnect()
    }
  }

  def echo(in: InputStream)(implicit codec: Codec): Unit = {
    val lineEcho = new LineEcho(in)(Codec.ISO8859)

    while (lineEcho.hasMore) {
      print(lineEcho.getLine)
    }
  }

  class LineEcho(in: InputStream)(implicit val codec: Codec) {
    val separator: String = System.getProperty("line.separator")

    var b = 0


    var hasMore: Boolean = true

    def getLine: String = {

      var n = in.available()

      val ar = java.nio.ByteBuffer.allocate(n)

      while (n > 0) {
        b = in.read()

        ar.put(b.toByte)
        n -= 1
        if (b == -1) hasMore = false
      }
      val s = new String(ar.array())
      s
    }
  }
}
