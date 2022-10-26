package pds;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;

import org.slf4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.thingworx.entities.utils.ThingUtilities;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.ThingworxBaseTemplateDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinitions;
import com.thingworx.metadata.annotations.ThingworxDataShapeDefinition;
import com.thingworx.metadata.annotations.ThingworxFieldDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.resources.Resource;
import com.thingworx.things.Thing;
import com.thingworx.things.repository.FileRepositoryThing;

@ThingworxBaseTemplateDefinition(name = "GenericThing")
@ThingworxConfigurationTableDefinitions(tables = {
		@ThingworxConfigurationTableDefinition(name = "Authorization", description = "", isMultiRow = false, ordinal = 0, dataShape = 
				@ThingworxDataShapeDefinition(fields = {
						@ThingworxFieldDefinition(name = "AccountName", description = "", baseType = "STRING", ordinal = 0, aspects = { }),
						@ThingworxFieldDefinition(name = "AccountKey", description = "", baseType = "STRING", ordinal = 1, aspects = { }),
						@ThingworxFieldDefinition(name = "Protocol", description = "HTTPS/HTTP", baseType = "STRING", ordinal =  2, aspects = { "defaultValue:HTTPS" })
				})) 
		})
public class PDSAzureResource extends Resource {
	private static final long serialVersionUID = -1668066256342468491L;
	private static Logger _logger = LogUtilities.getInstance().getScriptLogger(PDSAzureResource.class);
	
	public PDSAzureResource(){}
	
	private String getStorageConnectionString() {
		String accountName = getStringConfigurationSetting("Authorization", "AccountName");
		String accountKey = getStringConfigurationSetting("Authorization", "AccountKey");
		return "DefaultEndpointsProtocol=https;" + "AccountName=" + accountName + ";" + "AccountKey=" + accountKey;
	}
	
	@ThingworxServiceDefinition(name = "UploadFile", description = "Upload file to Azure", category = "Request")
	@ThingworxServiceResult(name = "result", description = "", baseType = "STRING")
	public String UploadFile(
			@ThingworxServiceParameter(name = "FileRepository", description = "File repository name", baseType = "THINGNAME") String fileRepository,
			@ThingworxServiceParameter(name = "Path", description = "Path to file with filename", baseType = "STRING", aspects = {"defaultValue:/" }) String path,
			@ThingworxServiceParameter(name = "Container", description = "Azure container name must be lower case", baseType = "STRING", aspects = { "defaultValue:archive" }) String container) throws URISyntaxException, StorageException, InvalidKeyException, IOException, Exception {
		
		Path filePath = getRealFilePath(fileRepository, path);
		uploadFile(filePath, container);
		
		return "File uploaded to: " + container;
	    	
	}

    public void uploadFile(Path path, String container) throws Exception {
    	_logger.info("PDS - Blob file uploading started");
    	String storageConnectionString = getStorageConnectionString();
        try {
            CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
            CloudBlobClient serviceClient = account.createCloudBlobClient();
            CloudBlobContainer blobContainer = serviceClient.getContainerReference(container);
            blobContainer.createIfNotExists();
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(path.getFileName().toString());
            blob.uploadFromFile(path.toAbsolutePath().toString());
            _logger.info("PDS - Blob file uploaded");
        } catch (StorageException | IOException | URISyntaxException | InvalidKeyException exception) {
        	_logger.error("PDS - Blob file failed: {}", exception.getMessage());
        	throw new Exception("PDS - Unable to upload blob file", exception);
        }
    }
    
    private Path getRealFilePath(String fileRepository, String filePath) throws Exception {
        Thing thing = ThingUtilities.findThing(fileRepository);
        if (thing == null) {
            throw new Exception("Thing " + fileRepository + " cannot be found");
        }
        if (!thing.getImplementedThingTemplates().contains("FileRepository")) {
            throw new Exception("Thing " + fileRepository + " is not a FileRepository");
        }
        FileRepositoryThing fileRepositoryThing = ((FileRepositoryThing) thing);
        return Paths.get(fileRepositoryThing.getRootPath(), filePath);
    }


}
