package com.bonree.brfs.email;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

/**
 * 1. make md5 with secret seed, resulting with lowercase string
 *
 * 2. password can be encrypted by AES with parameters:
 * ( ECB
 * ( PKCS5Padding
 * ( 128bit
 * ( utf-8
 * ( base64
 *
 * tool web: http://tool.chacuo.net/cryptaes
 */
public class EncryptedPasswordProvider {

    private final Cipher decryptCipher;
    private final String encodedWords;

    public EncryptedPasswordProvider(String secretKeySeed, String encodedWords) {
        if (secretKeySeed == null || secretKeySeed.isEmpty()) {
            throw new RuntimeException("secret seed is invalid");
        }

        this.encodedWords = encodedWords;

        try {
            decryptCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            decryptCipher.init(Cipher.DECRYPT_MODE, getKey(md5(secretKeySeed)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getPassword() {
        try {
            byte[] bytes = decryptCipher.doFinal(Base64.getDecoder().decode(encodedWords));
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String md5(String seed) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(seed.getBytes(StandardCharsets.UTF_8));
        return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();
    }

    private static SecretKey getKey(String password) {
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] finalBytes;
        if (passwordBytes.length > 32) {
            finalBytes = new byte[32];
            System.arraycopy(passwordBytes, 0, finalBytes, 0, finalBytes.length);
        } else if (passwordBytes.length > 24) {
            finalBytes = new byte[32];
            copyAndFillWithZero(passwordBytes, finalBytes);
        } else if (passwordBytes.length > 16) {
            finalBytes = new byte[24];
            copyAndFillWithZero(passwordBytes, finalBytes);
        } else {
            finalBytes = new byte[16];
            copyAndFillWithZero(passwordBytes, finalBytes);
        }

        return new SecretKeySpec(finalBytes, "AES");
    }

    private static void copyAndFillWithZero(byte[] src, byte[] target) {
        if (src.length > target.length) {
            throw new IllegalArgumentException("size of src should be less than size of target");
        }

        System.arraycopy(src, 0, target, 0, src.length);
        Arrays.fill(target, src.length, target.length, (byte) 0);
    }
}
