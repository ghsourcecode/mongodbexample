package cn.decom.database;

import cn.decom.connect.MongoDBConnectParams;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import jdk.nashorn.internal.runtime.JSType;
import jdk.nashorn.internal.runtime.Source;
import org.bson.Document;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.DefaultFeatureCollections;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.crs.DefaultGeocentricCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @date 2018/7/12 18:32
 */
public class Shp2MongoDB {

    public void readShp(String shpPath, Charset charset) {
        if (charset == null) {
            charset = Charset.forName("utf-8");
        }
        File shpFile = new File(shpPath);
        ShapefileDataStore dataStore = null;
        try {
            dataStore = new ShapefileDataStore(shpFile.toURI().toURL());
            dataStore.setCharset(charset);
            String type = dataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            FeatureCollection featureCollection = featureSource.getFeatures();
            FeatureIterator iterator = featureCollection.features();
            while (iterator.hasNext()) {
                SimpleFeature feature = (SimpleFeature) iterator.next();
                Geometry geom = (Geometry) feature.getDefaultGeometry();

                // 1: 第一种遍历属性方法，包括geom
                Collection<Property> properties = feature.getProperties();
                for (Property prop : properties) {
                    Object value = prop.getValue();
                    if (value instanceof Geometry) {

                    } else {
                        System.out.println("prop: " + value.toString());
                    }
                }

                // 2: 第二种遍历属性表，包括geom
                List attributes = feature.getAttributes();
                for (Object attr : attributes) {
                    if (attr instanceof Geometry) {

                    } else {
                        System.out.println("attr: " + attr.toString());
                    }
                }
            }

            dataStore.dispose();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取shp图dbf表
     *
     * @param shpPath
     * @param charset 指表字符集，防止乱码
     */
    public void readShpDbf(String shpPath, Charset charset) {
        if (charset == null) {
            charset = Charset.forName("utf-8");
        }
        try {
            ShpFiles shpFiles = new ShpFiles(shpPath);
            boolean isUseMemoryMappedBuffer = false;
            DbaseFileReader reader = new DbaseFileReader(shpFiles, isUseMemoryMappedBuffer, charset);
            DbaseFileHeader header = reader.getHeader();
            int numfields = header.getNumFields();
            while (reader.hasNext()) {
                DbaseFileReader.Row row = reader.readRow();
//                Object[] fileds = reader.readEntry();     //与reader.readRow()作用类似，即读取一行记录
                for (int i = 0; i < numfields; i++) {
                    String fieldName = header.getFieldName(i);
                    Object value = row.read(i);
//                    Object fieldValue = fileds[i];
                    System.out.println("..");
                }
            }

            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * geotools创建shp有几步：
     * 1、创建要素类型
     * 2、生成要素
     * 3、写入shp文件
     */
    public void writeShpToFile() {
        //1、创建featuretype
        SimpleFeatureTypeBuilder featureTypeBuilder = new SimpleFeatureTypeBuilder();
        featureTypeBuilder.setName("point");                        //需要设置featuretypebuilder的名字，不然空指针异常
        featureTypeBuilder.add("the_geom", Point.class);
        featureTypeBuilder.add("name", String.class);
        featureTypeBuilder.add("populate", String.class);
        featureTypeBuilder.add("GDP", Double.class);
        SimpleFeatureType featureType = featureTypeBuilder.buildFeatureType();

        //2、创建feature和featurecollection，将feature加入到featurecollection
        GeometryFactory factory = new GeometryFactory();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);

        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
        double lon = 115;
        double lat = 39;
        Point p1 = factory.createPoint(new Coordinate(lon, lat));
        String name1 = "beijing";
        String populate = "1000 w";
        Double gdp = Double.valueOf(2000);
        Object[] params = {p1, name1, populate, gdp};
        SimpleFeature feature = featureBuilder.buildFeature(null, params);

        featureCollection.add(feature);

        //3、创建shp，并写shp
        try {
            String projectResourcePath = this.getClass().getClassLoader().getResource(".").getPath();
            String targetFolderPath = projectResourcePath + "out";
            if(!new File(targetFolderPath).exists()) {
                new File(targetFolderPath).mkdirs();
            }
            String outShpPath = projectResourcePath + "out/out.shp";
            File out = new File(outShpPath);
            ShapefileDataStoreFactory shapefileDataStoreFactory = new ShapefileDataStoreFactory();
            HashMap<String, Serializable> shpParams = new HashMap<String, Serializable>();
            shpParams.put("url", out.toURI().toURL());
            shpParams.put("create spatial index", Boolean.TRUE);
            ShapefileDataStore shapefileDataStore = (ShapefileDataStore) shapefileDataStoreFactory.createNewDataStore(shpParams);
            shapefileDataStore.createSchema(featureType);
            shapefileDataStore.forceSchemaCRS(DefaultGeographicCRS.WGS84);

            Transaction transaction = new DefaultTransaction();
            String type = shapefileDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = shapefileDataStore.getFeatureSource(type);
            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
                featureStore.setTransaction(transaction);
                try {
                    featureStore.addFeatures(featureCollection);
                    transaction.commit();
                } catch (Exception e) {
                    transaction.rollback();
                } finally {
                    transaction.close();
                }
            }

            shapefileDataStore.dispose();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从shp创建shp
     */
    public void createShpFromShpFile() {
        try {
            //源shp
            String classPath = Shp2MongoDB.class.getClassLoader().getResource("").getPath();
            String sourceShpPath = classPath + "shp/china_province.shp";
            File sourceShpFile = new File(sourceShpPath);
            ShapefileDataStore sourceShapeDataStore = null;
            SimpleFeatureSource sourceFeatureSource = null;
            sourceShapeDataStore = new ShapefileDataStore(sourceShpFile.toURI().toURL());
            String type = sourceShapeDataStore.getTypeNames()[0];
            sourceFeatureSource = sourceShapeDataStore.getFeatureSource(type);

            //目标shp
            String outShpPath = classPath + "out/copyshp.shp";
            File targetShpFile = new File(outShpPath);
            ShapefileDataStoreFactory shapefileDataStoreFactory = new ShapefileDataStoreFactory();
            HashMap<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", targetShpFile.toURI().toURL());
            ShapefileDataStore targetShapefileDataStore = (ShapefileDataStore) shapefileDataStoreFactory.createNewDataStore(params);
            //设置属性
            SimpleFeatureType targetFeatureType = SimpleFeatureTypeBuilder.retype(sourceFeatureSource.getSchema(), DefaultGeographicCRS.WGS84);
            targetShapefileDataStore.createSchema(targetFeatureType);


            //构造featurewriter，写新shp文件要素
            String targetType = targetShapefileDataStore.getTypeNames()[0];
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = targetShapefileDataStore.getFeatureWriter(targetType, Transaction.AUTO_COMMIT);

            SimpleFeatureIterator iterator = sourceFeatureSource.getFeatures().features();
            try {
                while (iterator.hasNext()) {
                    SimpleFeature sourceFeature = iterator.next();
                    SimpleFeature targetFeature = writer.next();
                    targetFeature.setAttributes(sourceFeature.getAttributes());
                    writer.write();
                }
            } finally {
                iterator.close();
            }

            writer.close();
            sourceShapeDataStore.dispose();
            targetShapefileDataStore.dispose();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取shp，写入mongodb
     */
    public void readShpToMongoDB() {
        //将中国省行政边界导入到china_province集合（在关系数据库中称为表）
        String classPath = Shp2MongoDB.class.getClassLoader().getResource("").getPath();
        String shpPath = classPath + "shp/china_province.shp";

        //初始化database
        MongoClient mongoClient = new MongoClient(MongoDBConnectParams.URL + ":" + MongoDBConnectParams.PORT);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(MongoDBConnectParams.DATABASE);
        MongoCollection collection = mongoDatabase.getCollection(MongoDBConnectParams.COLLECTION);
        if (collection == null) {
            mongoDatabase.createCollection(MongoDBConnectParams.COLLECTION);
            collection = mongoDatabase.getCollection(MongoDBConnectParams.COLLECTION);
        }

        //shp读取
        File shpFile = new File(shpPath);
        try {
            ShapefileDataStore shapefileDataStore = new ShapefileDataStore(shpFile.toURI().toURL());
            String type = shapefileDataStore.getTypeNames()[0];
            shapefileDataStore.setCharset(Charset.forName("gbk"));
            SimpleFeatureSource featureSource = shapefileDataStore.getFeatureSource(type);
            FeatureCollection featureCollection = featureSource.getFeatures();
            FeatureIterator iterator = featureCollection.features();
            while (iterator.hasNext()) {
                SimpleFeature feature = (SimpleFeature) iterator.next();

//                Geometry geom = (Geometry) feature.getDefaultGeometry();
//
//                List attrs = feature.getAttributes();
//                for (Object obj : attrs) {
//                    if (obj instanceof Geometry) {
//                        String geomType = ((Geometry) obj).getGeometryType();
//                        System.out.println("geomType: " + geomType);
//                    } else {
//                        System.out.println("prop: " + obj.toString());
//                    }
//                }
//
//
//                Collection<Property> properties = feature.getProperties();
//                Iterator propertyIterator = properties.iterator();
//                while (propertyIterator.hasNext()) {
//                    Property property = (Property) propertyIterator.next();
//                    Name name = property.getName();
//                    System.out.println(name.getLocalPart());
//                    Object value = property.getValue();
//                    if (value instanceof Geometry) {
//                        String geomType = ((Geometry) value).getGeometryType();
//                        System.out.println("geomType: " + geomType);
//                    } else {
//                        System.out.println("property: " + name.getLocalPart() + property.getValue());
//                    }
//                }

                //转feature为featurejson，再转为bson，入mongodb
                FeatureJSON featureJSON = new FeatureJSON();
                StringWriter writer = new StringWriter();
                featureJSON.writeFeature(feature, writer);
                String jsonString = writer.toString();
                Document document = Document.parse(jsonString);
                collection.insertOne(document);
            }

            iterator.close();
            shapefileDataStore.dispose();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readShpFromMongoDB() {
        MongoClient client = new MongoClient(MongoDBConnectParams.URL + ":" + MongoDBConnectParams.PORT);
        MongoDatabase database = client.getDatabase(MongoDBConnectParams.DATABASE);
        MongoCollection collection = database.getCollection(MongoDBConnectParams.COLLECTION);

        if (collection == null)
            return;

        Iterator<Document> iterator = collection.find().iterator();
        while (iterator.hasNext()) {
            Document document = iterator.next();
            String jsonString = document.toJson();
            FeatureJSON featureJSON = new FeatureJSON();
            try {
                SimpleFeature feature = featureJSON.readFeature(jsonString);
                Geometry geom = (Geometry) feature.getDefaultGeometry();
                List attrs = feature.getAttributes();
                for (Object obj : attrs) {
                    if (obj instanceof Geometry) {
                        String geomType = ((Geometry) obj).getGeometryType();
                        System.out.println("geomType: " + geomType);
                    } else {
                        System.out.println("prop: " + obj.toString());
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("sdafjl");
        }

    }


    public static void main(String[] args) {
        Shp2MongoDB shp2MongoDB = new Shp2MongoDB();
//        shp2MongoDB.readShpToMongoDB();
//        shp2MongoDB.readShpFromMongoDB();
//        shp2MongoDB.writeShpToFile();
        shp2MongoDB.createShpFromShpFile();

        String classPath = Shp2MongoDB.class.getClassLoader().getResource("").getPath();
        String shpPath = classPath + "shp/china_province.shp";
//        shp2MongoDB.readShpDbf(shpPath, Charset.forName("gbk"));

    }


}
