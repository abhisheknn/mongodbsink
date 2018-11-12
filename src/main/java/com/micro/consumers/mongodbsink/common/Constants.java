package com.micro.consumers.mongodbsink.common;

public class Constants {

	public static final String CONTAINER_DETAILS_TOPIC = "container_details";
	public static final String CONTAINERID_TO_IMAGEID_TOPIC = "containerid_to_imageid";
	public static final String CONTAINER_TO_MOUNTS_TOPIC = "container_to_mount";
	public static final String CONTAINER_DETAILS_CONSUMER_GROUP_ID = "container_details_consumer";
	public static final String CONTAINERID_TO_IMAGEID_CONSUMER_GROUP_ID = "container_to_image_id_consumer";
	public static final String CONTAINER_TO_MOUNT_CONSUMER_GROUP_ID = "container_to_mount_id_consumer";
	public static final String KAFKABROKER = System.getenv("KAFKABROKER");
	public static final String MONGODB_DATABASE = System.getenv("MONGODB_DATABASE");
	public static final String MONGODBUSERNAME = System.getenv("MONGODBUSERNAME");
	public static final String MONGODBPASSWORD = System.getenv("MONGODBPASSWORD");
	public static final int MONGODBPORT = Integer.parseInt(System.getenv("MONGODBPORT"));
	public static final String MONGODBHOST = System.getenv("MONGODBHOST");

}
