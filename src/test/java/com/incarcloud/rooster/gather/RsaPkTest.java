package com.incarcloud.rooster.gather;

import com.google.gson.reflect.TypeToken;
import com.incarcloud.rooster.share.Constants;
import com.incarcloud.rooster.util.GsonFactory;
import org.junit.Test;

import java.util.Map;

/**
 * RsaPkTest
 */
public class RsaPkTest {

    @Test
    public void testStringToMap() {
        // RSA私钥
        String rsaPrivateKeyString = "{\n" +
                "  \"e\": \"bbrxPq894DpXs7XgH6UgyYcB7xri+4UiVsNWFXJwwrA+nf92zbZIfzu1pyyiaCNRvt7hH8Pvnq/vtSeBDptUlR77pe71kdDcosI5Le7yjgP/Et0epHqWnusKpcqSshcJfP+u+tS61BljAuN9f9XSR+k2p0YhvTQJJEvaD9JQQrE=\",\n" +
                "  \"n\": \"ALG7YmTar/YHt+lPGSCkZsqWORuG/ebrukbST/O0KrODi4XaWONYjY43yKUfM6UufU/wNT0jL7v4WM/FbTqNQzBLNW7ut+hCYbUZLYwgGsOagla/OrXwN8Puy6F+f0OxVs2wyVIYDHN4PreFnxG7C28puhz65nKvk+7lxx0oZUWj\"\n" +
                "}";
        Map<String, Object> mapPrivateKey = GsonFactory.newInstance().createGson().fromJson(rsaPrivateKeyString, new TypeToken<Map<String, Object>>() {}.getType());
        System.out.println(mapPrivateKey.get(Constants.RSADataMapKey.N).toString());
        System.out.println(mapPrivateKey.get(Constants.RSADataMapKey.E).toString());

        // RSA公钥
        String rsaPublicKeyString = "{\n" +
                "  \"e\": 65537,\n" +
                "  \"n\": \"sbtiZNqv9ge36U8ZIKRmypY5G4b95uu6RtJP87Qqs4OLhdpY41iNjjfIpR8zpS59T/A1PSMvu/hYz8VtOo1DMEs1bu636EJhtRktjCAaw5qCVr86tfA3w+7LoX5/Q7FWzbDJUhgMc3g+t4WfEbsLbym6HPrmcq+T7uXHHShlRaM=\"\n" +
                "}";
        Map<String, Object> mapPublicKey = GsonFactory.newInstance().createGson().fromJson(rsaPublicKeyString, new TypeToken<Map<String, Object>>() {}.getType());
        System.out.println(mapPublicKey.get(Constants.RSADataMapKey.N).toString());
        System.out.println(Double.valueOf(mapPublicKey.get(Constants.RSADataMapKey.E).toString()).longValue());
    }
}
