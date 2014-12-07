package io.scalding.approximations

import java.io._

import com.twitter.algebird.{CMS, BF}

/**
 * A utility package object to demonstrate how to serialize algebird's
 * approximation
 */
package object Utils {

  // Serialize a Bloom Filter into a String
  def serialize(bf: BF): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(stream)
    out.writeObject(bf)
    out.close()
    stream.close()

    stream.toByteArray
  }

  // Serialize a Count-min Sketch into a String
  def serialize(cms: CMS) = {
    val stream = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(stream)
    out.writeObject(cms)
    out.close()
    stream.close()
    new String(stream.toByteArray)
  }


  def deserialize[T](byteArray: Array[Byte]): T = {
    val is = new ObjectInputStream(new ByteArrayInputStream(byteArray))
    is.readObject().asInstanceOf[T]
  }
}
