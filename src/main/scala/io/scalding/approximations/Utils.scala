package io.scalding.approximations

import java.io._

import com.twitter.algebird.{CMS, BF}

/**
 * A utility package object for serializing approximation data structures to disk
 */
package object Utils {

  // Serialize a Bloom Filter in a byte array
  def serialize(dataStructure: Any): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(stream)
    out.writeObject(dataStructure)
    out.close()
    stream.close()

    stream.toByteArray
  }

  // Serialize a Count-min Sketch into a String
//  def serialize(cms: CMS): Array[Byte] = {
//    val stream = new ByteArrayOutputStream()
//    val out = new ObjectOutputStream(stream)
//    out.writeObject(cms)
//    out.close()
//    stream.close()
//
//    stream.toByteArray
//  }


  def deserialize[T](byteArray: Array[Byte]): T = {
    val is = new ObjectInputStream(new ByteArrayInputStream(byteArray))
    is.readObject().asInstanceOf[T]
  }
}
