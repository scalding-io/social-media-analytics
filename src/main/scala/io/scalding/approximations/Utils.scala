package io.scalding.approximations

import java.io.{ObjectOutputStream, ByteArrayOutputStream}

import com.twitter.algebird.{CMS, BF}

/**
 * A utility package object to demonstrate how to serialize algebird's
 * approximation
 */
package object Utils {

  // Serialize a Bloom Filter into a String
  def serialize(bf: BF) = {
    val stream = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(stream)
    out.writeObject(bf)
    out.close()
    stream.close()
    new String(stream.toByteArray)
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

  // Serialize a Bloom Filter using Kryo
  def kryoSerialize(bf: BF) = {
//    import com.twitter.chill.KryoInjection
//    val bytes:  Array[Byte]    = new KryoInjection(someItem)
//    val tryDecode: scala.util.Try[Any] = KryoInjection.invert(bytes)

    "todo"
  }


}
