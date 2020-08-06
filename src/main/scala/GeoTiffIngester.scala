import geotrellis.layer._
import geotrellis.raster._
import geotrellis.proj4._
import geotrellis.raster.geotiff._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.spark._
import geotrellis.spark.store.s3._
import geotrellis.store.LayerId
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.store.s3._
import org.apache.spark.{SparkConf, SparkContext}

object GeoTiffIngester {
  val sparkMaster: String = "local[8]"
  val s3Bucket: String = "some-bucket"
  val baseZoomLevel: Int = 21
  val layerName: String = "do_s13_7771_13_2013_3"
  val geoimageryPath: String = s"s3://${s3Bucket}/sample_geotiffs/do_s13_7771_13.2013.tif"
  val attributeStorePath: String = "gt3-ingest-geotiff-demo-1"
  val layerStorePath: String = "gt3-ingest-geotiff-demo-1"

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().set("spark.app.name", "gt3-ingest-geotiff-demo").setMaster(sparkMaster)
    implicit  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

    val geotiffPath: GeoTiffPath                 = GeoTiffPath(geoimageryPath)
    val geoTiffRasterSource: GeoTiffRasterSource = GeoTiffRasterSource(geotiffPath)
    try {
      val crs: CRS = geoTiffRasterSource.crs
    } catch {
      case e: MalformedGeoTiffException => {
        println(s"${geoimageryPath} is not a valid Geotiff")
        throw e
      }
    }
    val floatingLayoutScheme: FloatingLayoutScheme = FloatingLayoutScheme(1000)

    val numberOfBands: Int = geoTiffRasterSource.bandCount
    val layout: LayoutDefinition =
      floatingLayoutScheme.levelFor(geoTiffRasterSource.extent, geoTiffRasterSource.cellSize).layout

    // TODO - To be used by task 24568 to reproject to r  pherical Mercator
    //val geoTiffRasterSourceReprojected: RasterSource =
    //  geoTiffRasterSource.reprojectToGrid(crs, layout)

    val rasterSourceRDD = RasterSourceRDD.spatial(geoTiffRasterSource, layout)
    println("Computing count...")
    val rasterSourceRDDCount = rasterSourceRDD.count()
    println(s"rasterSourceRDD count is ${rasterSourceRDDCount}")

    val s3AttributeStore: S3AttributeStore = new S3AttributeStore(s3Bucket, attributeStorePath)
    val s3LayerWriter = new S3LayerWriter(
      s3AttributeStore,
      s3Bucket,
      layerStorePath
    )

    val layerId = LayerId(layerName, baseZoomLevel)
    s3LayerWriter.write(layerId, rasterSourceRDD, ZCurveKeyIndexMethod)

    println("Done!")

    Thread.sleep(5 * 1000 * 60)


  }

}
