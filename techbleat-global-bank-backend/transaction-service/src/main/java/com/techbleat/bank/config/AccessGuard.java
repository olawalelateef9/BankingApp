package com.techbleat.bank.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * Account-level authorization: a customer may only act on the account
 * registered to their login email; members of the admin group may act on any.
 */
@Component
public class AccessGuard {

    private final JdbcTemplate jdbcTemplate;

    @Value("${oidc.admin-group:bank-admins}")
    private String adminGroup;

    @Value("${oidc.groups-claim:cognito:groups}")
    private String groupsClaim;

    public AccessGuard(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void requireAccess(String userId) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (!(auth instanceof JwtAuthenticationToken jwtAuth)) {
            return; // authentication disabled (no OIDC_ISSUER)
        }

        Jwt jwt = jwtAuth.getToken();
        List<String> groups = jwt.getClaimAsStringList(groupsClaim);
        if (groups != null && groups.contains(adminGroup)) {
            return;
        }

        String email = jwt.getClaimAsString("email");
        if (email == null) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Token has no email claim");
        }

        List<String> ownIds = jdbcTemplate.queryForList(
                "SELECT id FROM users WHERE lower(email) = lower(?)", String.class, email);
        if (!ownIds.contains(userId)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Not allowed to access this account");
        }
    }
}
