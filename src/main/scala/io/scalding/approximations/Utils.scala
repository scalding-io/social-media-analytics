package io.scalding.approximations

import java.io._

/**
 * A utility package object for serializing approximation data structures to disk
 */
object Utils {

  // Serialize a Bloom Filter in a byte array
  def serialize(dataStructure: Any): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(stream)
    out.writeObject(dataStructure)
    out.close()
    stream.close()

    stream.toByteArray
  }

  def deserialize[T](byteArray: Array[Byte]): T = {
    val is = new ObjectInputStream(new ByteArrayInputStream(byteArray))
    is.readObject().asInstanceOf[T]
  }

}

