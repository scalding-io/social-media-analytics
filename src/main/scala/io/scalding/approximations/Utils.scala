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

  /**
   * A `helper` method. Using this method we can `wrap` a block of code and count the time
   * required to complete the execution of that block of code
   */
  def withTimeCalc(blockDescr: String)( block : => Unit): Long = {
    println(blockDescr)
    val start = System.currentTimeMillis()
    var end = 0l
    try {
      block
    } finally {
      end = System.currentTimeMillis()
    }
    end - start
  }

}

